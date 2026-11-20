#!/usr/bin/env python3
"""
Автоматический прогон пяти экспериментов ЛР 5.

Скрипт сам запускает три процесса, останавливает и перезапускает основной
узел, замедляет его ответы и показывает, какой узел обслужил каждый заказ.

Запуск:
    python3 run_experiments.py
"""

from __future__ import annotations

import argparse
import json
import os
import subprocess
import sys
import time

import requests

PY = sys.executable
LOGS = ("primary.log", "replica.log", "orders.log")


def head(title: str) -> None:
    print()
    print("=" * 76)
    print(title)
    print("=" * 76)


def sub(title: str) -> None:
    print()
    print("-" * 76)
    print(title)
    print("-" * 76)


def call(method: str, url: str, payload: dict | None = None) -> dict | None:
    if payload is None:
        print(f"$ curl -i {url}")
    else:
        body = json.dumps(payload, ensure_ascii=False)
        print(f"$ curl -i -X {method} {url} \\")
        print("       -H 'Content-Type: application/json' \\")
        print(f"       -d '{body}'")
    started = time.perf_counter()
    try:
        response = requests.request(method, url, json=payload, timeout=30)
    except requests.exceptions.RequestException as exc:
        print(f"  !! запрос не выполнен: {exc.__class__.__name__}")
        return None
    elapsed = time.perf_counter() - started

    print(f"HTTP/1.1 {response.status_code} {response.reason}   "
          f"(время ответа {elapsed * 1000:.0f} мс)")
    print()
    try:
        body = response.json()
        print(json.dumps(body, ensure_ascii=False, indent=2))
    except ValueError:
        print(response.text)
        body = None
    print()
    return body


def start_catalog(role: str, host: str, port: int, extra: list[str],
                  log_path: str) -> subprocess.Popen:
    cmd = [PY, "catalog_service.py", "--role", role, "--host", host,
           "--port", str(port), *extra, "--log-file", log_path]
    return subprocess.Popen(cmd, stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)


def stop(proc: subprocess.Popen | None) -> None:
    if proc is None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


def summary(order: dict | None) -> str:
    if not order:
        return "заказ не создан"
    return (f"source={order.get('catalog_source')}, "
            f"version={order.get('catalog_version')}, "
            f"price={order.get('price')}")


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--host", default="127.0.0.1")
    ap.add_argument("--primary-port", type=int, default=5001)
    ap.add_argument("--replica-port", type=int, default=5002)
    ap.add_argument("--orders-port", type=int, default=5003)
    ap.add_argument("--timeout", type=float, default=1.0,
                    help="таймаут обращения заказов к основному узлу, с")
    ap.add_argument("--slow", type=float, default=3.0,
                    help="задержка ответа медленного узла, с")
    ap.add_argument("--replica-delay", type=float, default=5.0,
                    help="задержка асинхронной доставки на реплику, с")
    a = ap.parse_args()

    primary = f"http://{a.host}:{a.primary_port}"
    replica = f"http://{a.host}:{a.replica_port}"
    orders = f"http://{a.host}:{a.orders_port}"

    for path in LOGS:
        try:
            os.remove(path)
        except OSError:
            pass

    primary_args = ["--replica-url", replica,
                    "--replica-delay", str(a.replica_delay)]

    replica_proc = start_catalog("replica", a.host, a.replica_port, [],
                                 "replica.log")
    primary_proc = start_catalog("primary", a.host, a.primary_port,
                                 primary_args, "primary.log")
    orders_proc = subprocess.Popen(
        [PY, "order_service.py", "--host", a.host, "--port", str(a.orders_port),
         "--primary-url", primary, "--replica-url", replica,
         "--timeout", str(a.timeout), "--log-file", "orders.log"],
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    time.sleep(2.5)

    results: dict[str, str] = {}
    try:
        head("ИСХОДНОЕ СОСТОЯНИЕ")
        print(f"Catalog Primary : {primary}")
        print(f"Catalog Replica : {replica}")
        print(f"Order Service   : {orders}")
        print(f"Таймаут обращения заказов к основному узлу: {a.timeout} с")
        print()
        call("GET", f"{orders}/catalog-status")

        sub("Подготовка: обновляем цену до 450000 в синхронном режиме, "
            "чтобы оба узла были на version 2")
        call("PUT", f"{primary}/products/1",
             {"price": 450000, "mode": "sync"})

        # ---------------------------------------------------------------
        head("ЭКСПЕРИМЕНТ 1. Нормальная работа")
        o1 = call("POST", f"{orders}/orders", {"product_id": 1, "quantity": 1})
        results["exp1"] = summary(o1)
        print("Основной узел доступен — заказ обслужен им.")

        # ---------------------------------------------------------------
        head("ЭКСПЕРИМЕНТ 2. Основной узел остановлен")
        sub("Останавливаем Catalog Primary")
        stop(primary_proc)
        primary_proc = None
        time.sleep(1.0)
        call("GET", f"{orders}/catalog-status")

        sub("Создаём заказ")
        o2 = call("POST", f"{orders}/orders", {"product_id": 1, "quantity": 1})
        results["exp2"] = summary(o2)
        print("Основной узел не ответил — чтение выполнено с реплики, "
              "обслуживание не прервалось.")

        # ---------------------------------------------------------------
        head("ЭКСПЕРИМЕНТ 3. Восстановление основного узла")
        sub("Запускаем Catalog Primary заново")
        primary_proc = start_catalog(
            "primary", a.host, a.primary_port,
            primary_args + ["--sync-from", replica], "primary.log")
        time.sleep(2.5)
        print("Данные хранятся в памяти, поэтому перезапущенный узел "
              "забирает актуальное состояние с реплики (--sync-from).")
        call("GET", f"{orders}/catalog-status")

        sub("Создаём заказ")
        o3 = call("POST", f"{orders}/orders", {"product_id": 1, "quantity": 1})
        results["exp3"] = summary(o3)
        print("Основной узел снова отвечает — заказы вернулись к нему.")

        # ---------------------------------------------------------------
        head("ЭКСПЕРИМЕНТ 4. Медленный основной узел")
        sub(f"Устанавливаем задержку ответа {a.slow:.0f} с при таймаухе "
            f"{a.timeout:.0f} с")
        call("POST", f"{primary}/admin/response-delay", {"seconds": a.slow})

        sub("Создаём заказ — узел ЗАПУЩЕН, но не укладывается в таймаут")
        o4 = call("POST", f"{orders}/orders", {"product_id": 1, "quantity": 1})
        results["exp4"] = summary(o4)
        print("Переключение произошло, хотя основной узел работает. "
              "Таймаут даёт лишь подозрение на отказ, а не доказательство.")

        sub("Снимаем задержку")
        call("POST", f"{primary}/admin/response-delay", {"seconds": 0})

        # ---------------------------------------------------------------
        head("ЭКСПЕРИМЕНТ 5. Отстающая реплика")
        sub(f"Меняем цену 450000 → 400000 в асинхронном режиме "
            f"(доставка через {a.replica_delay:.0f} с)")
        call("PUT", f"{primary}/products/1",
             {"price": 400000, "mode": "async"})

        sub("Немедленно останавливаем основной узел — "
            "до того, как обновление дойдёт до реплики")
        stop(primary_proc)
        primary_proc = None
        time.sleep(0.5)

        sub("Создаём заказ")
        o5 = call("POST", f"{orders}/orders", {"product_id": 1, "quantity": 1})
        results["exp5"] = summary(o5)
        print("Переключение сработало, но реплика отдала СТАРУЮ цену 450000: "
              "новая версия до неё не доехала и уже не доедет.")

        # ---------------------------------------------------------------
        head("ТАБЛИЦА РЕЗУЛЬТАТОВ")
        rows = [
            ("1. Нормальная работа", "работает", results["exp1"],
             "актуальные данные"),
            ("2. Узел остановлен", "не отвечает", results["exp2"],
             "чтение продолжается"),
            ("3. Узел восстановлен", "работает", results["exp3"],
             "возврат к основному"),
            ("4. Медленный узел", f"задержка {a.slow:.0f} с", results["exp4"],
             "ложное подозрение"),
            ("5. Отставание реплики", "остановлен", results["exp5"],
             "старая цена"),
        ]
        print(f"{'Эксперимент':<24}{'Primary':<16}"
              f"{'Чем обслужен заказ':<42}{'Результат':<22}")
        print("-" * 104)
        for name, state, got, outcome in rows:
            print(f"{name:<24}{state:<16}{got:<42}{outcome:<22}")

        # ---------------------------------------------------------------
        head("ОБЪЕДИНЁННЫЙ ЖУРНАЛ ТРЁХ СЕРВИСОВ")
        merged: list[str] = []
        for path in LOGS:
            try:
                with open(path, encoding="utf-8") as fh:
                    merged.extend(ln.rstrip() for ln in fh if ln.strip())
            except OSError:
                pass
        merged.sort(key=lambda line: line[:8])
        for line in merged:
            print(line)
        with open("combined_log.txt", "w", encoding="utf-8") as fh:
            fh.write("\n".join(merged) + "\n")
        print()
        print("Объединённый журнал сохранён в combined_log.txt")
    finally:
        stop(primary_proc)
        stop(replica_proc)
        stop(orders_proc)
        print()
        print("Все процессы остановлены.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
