#!/usr/bin/env python3
"""
Лабораторная работа 5 — сервис каталога с репликацией и отказами.

Развитие ЛР 4: добавлены управляемая задержка ответа на чтение (для
имитации перегруженного узла) и восстановление состояния с реплики
после перезапуска.

Один и тот же файл запускается в двух ролях:

    --role primary   основной каталог: принимает чтение и обновления,
                     рассылает изменения реплике
    --role replica    копия: принимает изменения от основного узла,
                     подтверждает их (ACK) и обслуживает чтение

Запуск:
    python3 catalog_service.py --role primary --port 5001 \\
            --replica-url http://127.0.0.1:5002
    python3 catalog_service.py --role replica --port 5002
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading
import time

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
app.json.ensure_ascii = False
app.json.sort_keys = False

# ---------------------------------------------------------------------------
# Состояние узла
# ---------------------------------------------------------------------------
ROLE = "primary"
REPLICA_URL = "http://127.0.0.1:5002"
REPLICA_DELAY = 5.0        # задержка доставки в асинхронном режиме, с
REPLICA_TIMEOUT = 5.0      # таймаут запроса primary → replica, с
ACK_DELAY = 0.0            # задержка ответа реплики: имитация сети между узлами
READ_DELAY = 0.0           # задержка ответа на чтение: имитация перегруженного узла

# Товар хранится в памяти процесса. Поле version растёт на единицу
# при каждом обновлении на основном узле.
_products: dict[int, dict] = {}
_lock = threading.Lock()

log = logging.getLogger("catalog")


def seed() -> None:
    """Начальные данные: одинаковые на основном узле и на реплике."""
    with _lock:
        _products[1] = {
            "id": 1,
            "name": "Laptop",
            "price": 500000,
            "stock": 10,
            "version": 1,
        }


# ---------------------------------------------------------------------------
# Доставка изменений на реплику
# ---------------------------------------------------------------------------
def push_to_replica(snapshot: dict) -> tuple[bool, str]:
    """
    Отправка новой версии товара на реплику.

    Возвращает (успех, описание). Используется и в синхронном режиме,
    где основной узел ждёт ответа, и в асинхронном, где отправка идёт
    в фоновом потоке.
    """
    url = f"{REPLICA_URL}/internal/replicate"
    try:
        response = requests.post(url, json=snapshot, timeout=REPLICA_TIMEOUT)
    except requests.exceptions.ConnectionError:
        return False, "реплика недоступна (нет соединения)"
    except requests.exceptions.Timeout:
        return False, f"реплика не ответила за {REPLICA_TIMEOUT} с"
    except requests.exceptions.RequestException as exc:
        return False, f"ошибка запроса к реплике: {exc.__class__.__name__}"

    if response.status_code != 200:
        return False, f"реплика вернула код {response.status_code}"

    body = response.json()
    if not body.get("ack"):
        return False, "реплика не подтвердила применение"
    return True, f"ACK от реплики, version={body.get('version')}"


def delayed_push(snapshot: dict, delay: float) -> None:
    """Фоновая доставка: ждём задержку, затем отправляем на реплику."""
    log.info("АСИНХРОННО: доставка version=%s начнётся через %.0f с",
             snapshot["version"], delay)
    time.sleep(delay)
    ok, detail = push_to_replica(snapshot)
    if ok:
        log.info("АСИНХРОННО: version=%s доставлена на реплику — %s",
                 snapshot["version"], detail)
    else:
        log.error("АСИНХРОННО: доставка version=%s не удалась — %s",
                  snapshot["version"], detail)


# ---------------------------------------------------------------------------
# Обработчики ошибок
# ---------------------------------------------------------------------------
@app.errorhandler(404)
def handle_404(error):
    return jsonify({"error": "not_found",
                    "message": "Запрошенный ресурс не найден"}), 404


@app.errorhandler(405)
def handle_405(error):
    return jsonify({"error": "method_not_allowed",
                    "message": "Метод не поддерживается для этого адреса"}), 405


# ---------------------------------------------------------------------------
# Чтение
# ---------------------------------------------------------------------------
@app.get("/products/<int:product_id>")
def get_product(product_id: int):
    """GET /products/<id> — текущее состояние товара на этом узле."""
    if READ_DELAY:
        # Узел работает, но отвечает медленно. Для клиента с коротким
        # таймаутом это неотличимо от остановленного сервиса.
        log.warning("Чтение задержано на %.1f с (имитация перегрузки)",
                    READ_DELAY)
        time.sleep(READ_DELAY)
    with _lock:
        product = _products.get(product_id)
    if product is None:
        return jsonify({"error": "not_found",
                        "message": f"Товар с id={product_id} не найден"}), 404
    answer = dict(product)
    answer["served_by"] = ROLE          # видно, какой узел ответил
    log.info("Чтение товара id=%s → version=%s, price=%s, stock=%s",
             product_id, product["version"], product["price"], product["stock"])
    return jsonify(answer), 200


@app.get("/products")
def list_products():
    with _lock:
        items = [_products[k] for k in sorted(_products)]
    return jsonify({"role": ROLE, "count": len(items), "items": items}), 200


@app.post("/admin/response-delay")
def set_response_delay():
    """
    POST /admin/response-delay — задать задержку ответа на чтение.

    Тело: {"seconds": 3}. Нужно для эксперимента «медленный узел»:
    сервис остаётся запущенным, но перестаёт укладываться в таймаут
    клиента. Перезапускать процесс для этого не требуется.
    """
    global READ_DELAY
    data = request.get_json(silent=True) or {}
    seconds = data.get("seconds", 0)
    if not isinstance(seconds, (int, float)) or isinstance(seconds, bool) \
            or seconds < 0:
        return jsonify({"error": "validation_error",
                        "message": "Поле 'seconds' должно быть неотрицательным числом"}), 400
    READ_DELAY = float(seconds)
    log.info("Задержка ответа на чтение установлена: %.1f с", READ_DELAY)
    return jsonify({"role": ROLE, "response_delay": READ_DELAY}), 200


@app.get("/health")
def health():
    with _lock:
        versions = {pid: p["version"] for pid, p in _products.items()}
    return jsonify({"service": "catalog", "role": ROLE,
                    "status": "ok", "versions": versions,
                    "response_delay": READ_DELAY}), 200


# ---------------------------------------------------------------------------
# Обновление (только основной узел)
# ---------------------------------------------------------------------------
@app.put("/products/<int:product_id>")
def update_product(product_id: int):
    """
    PUT /products/<id> — обновление товара.

    Тело: изменяемые поля (price и/или stock) и режим репликации:
        {"price": 450000, "mode": "async"}
        {"stock": 5, "mode": "sync"}
    """
    if ROLE != "primary":
        return jsonify({
            "error": "read_only_replica",
            "message": "Реплика принимает только чтение; обновляйте основной узел",
        }), 403

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "bad_request",
                        "message": "Ожидается тело запроса в формате JSON"}), 400

    mode = str(data.get("mode", "async")).lower()
    if mode not in ("async", "sync"):
        return jsonify({"error": "validation_error",
                        "message": "Поле 'mode' должно быть 'async' или 'sync'"}), 400

    updates: dict[str, object] = {}
    for field in ("price", "stock"):
        if field in data:
            value = data[field]
            if not isinstance(value, (int, float)) or isinstance(value, bool) \
                    or value < 0:
                return jsonify({
                    "error": "validation_error",
                    "message": f"Поле '{field}' должно быть неотрицательным числом",
                }), 400
            updates[field] = value
    if "name" in data and isinstance(data["name"], str) and data["name"].strip():
        updates["name"] = data["name"].strip()

    if not updates:
        return jsonify({
            "error": "validation_error",
            "message": "Нужно передать хотя бы одно поле: price, stock или name",
        }), 400

    # --- применение изменения на основном узле ---
    with _lock:
        product = _products.get(product_id)
        if product is None:
            return jsonify({"error": "not_found",
                            "message": f"Товар с id={product_id} не найден"}), 404
        product.update(updates)
        product["version"] += 1          # версия растёт при каждом обновлении
        snapshot = dict(product)

    log.info("ОБНОВЛЕНИЕ id=%s принято: %s → version=%s, режим %s",
             product_id, updates, snapshot["version"], mode)

    # --- доставка на реплику ---
    if mode == "async":
        # Основной узел сохранил изменение и СРАЗУ отвечает SUCCESS.
        # Реплика получит версию позже, в фоновом потоке.
        threading.Thread(target=delayed_push,
                         args=(snapshot, REPLICA_DELAY),
                         daemon=True).start()
        log.info("SUCCESS отправлен клиенту НЕ дожидаясь реплики (async)")
        return jsonify({
            "status": "SUCCESS",
            "mode": "async",
            "product": snapshot,
            "replication": f"доставка на реплику через {REPLICA_DELAY:.0f} с",
        }), 200

    # Синхронный режим: сначала реплика, потом ответ клиенту.
    log.info("СИНХРОННО: отправляю version=%s на реплику и жду ACK",
             snapshot["version"])
    started = time.perf_counter()
    ok, detail = push_to_replica(snapshot)
    waited = time.perf_counter() - started

    if not ok:
        log.error("СИНХРОННО: подтверждение не получено — %s", detail)
        return jsonify({
            "status": "FAILED",
            "mode": "sync",
            "product": snapshot,
            "replication": detail,
            "message": "Изменение применено на основном узле, но реплика "
                       "его не подтвердила",
        }), 503

    log.info("СИНХРОННО: %s — только теперь отправляю SUCCESS (ожидание %.3f с)",
             detail, waited)
    return jsonify({
        "status": "SUCCESS",
        "mode": "sync",
        "product": snapshot,
        "replication": detail,
        "waited_for_ack_seconds": round(waited, 3),
    }), 200


# ---------------------------------------------------------------------------
# Приём изменений (только реплика)
# ---------------------------------------------------------------------------
@app.post("/internal/replicate")
def replicate():
    """
    POST /internal/replicate — приём новой версии товара от основного узла.

    Отвечает подтверждением (ACK). Версия применяется только если она
    новее текущей: это защищает от применения устаревшего снимка, если
    сообщения придут не в том порядке.
    """
    if ROLE != "replica":
        return jsonify({"error": "wrong_role",
                        "message": "Этот узел не является репликой"}), 403

    snapshot = request.get_json(silent=True)
    if not isinstance(snapshot, dict) or "id" not in snapshot \
            or "version" not in snapshot:
        return jsonify({"error": "bad_request",
                        "message": "Ожидается снимок товара с полями id и version"}), 400

    if ACK_DELAY:
        # В реальной системе узлы стоят на разных машинах, и подтверждение
        # идёт по сети. Здесь эта задержка задаётся явно.
        time.sleep(ACK_DELAY)

    product_id = snapshot["id"]
    incoming = snapshot["version"]

    with _lock:
        current = _products.get(product_id)
        if current is not None and current["version"] >= incoming:
            log.warning("РЕПЛИКА: version=%s не применена, текущая %s — "
                        "устаревший или повторный снимок",
                        incoming, current["version"])
            return jsonify({"ack": True, "applied": False,
                            "version": current["version"]}), 200
        _products[product_id] = dict(snapshot)

    log.info("РЕПЛИКА: применена version=%s (price=%s, stock=%s), отправляю ACK",
             incoming, snapshot.get("price"), snapshot.get("stock"))
    return jsonify({"ack": True, "applied": True, "version": incoming}), 200


# ---------------------------------------------------------------------------
def catch_up_from(url: str) -> None:
    """
    Восстановление состояния после перезапуска.

    Данные хранятся в памяти, поэтому перезапущенный узел стартует пустым.
    Он забирает актуальные записи у соседа и принимает те, чья версия
    не ниже локальной. В реальных системах восстановление идёт из журнала
    или от другой реплики — здесь показан тот же принцип в простом виде.
    """
    try:
        response = requests.get(f"{url.rstrip('/')}/products", timeout=3.0)
        response.raise_for_status()
        items = response.json().get("items", [])
    except (requests.exceptions.RequestException, ValueError) as exc:
        log.warning("Восстановление с %s не удалось (%s), "
                    "остаюсь с начальными данными", url, exc.__class__.__name__)
        return

    restored = 0
    with _lock:
        for item in items:
            pid = item.get("id")
            if pid is None:
                continue
            current = _products.get(pid)
            if current is None or item.get("version", 0) >= current["version"]:
                _products[pid] = dict(item)
                restored += 1
    log.info("Восстановлено записей с %s: %d", url, restored)


def setup_logging(role: str, log_file: str | None) -> None:
    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if log_file:
        handlers.append(logging.FileHandler(log_file, encoding="utf-8"))
    logging.basicConfig(
        level=logging.INFO,
        format=f"%(asctime)s [{role.upper():<7}] %(message)s",
        datefmt="%H:%M:%S",
        handlers=handlers,
    )
    # убрать стандартный шум werkzeug про каждый запрос
    logging.getLogger("werkzeug").setLevel(logging.WARNING)


def main() -> None:
    global ROLE, REPLICA_URL, REPLICA_DELAY, REPLICA_TIMEOUT, ACK_DELAY, READ_DELAY

    parser = argparse.ArgumentParser(description="Каталог с репликацией")
    parser.add_argument("--role", choices=["primary", "replica"],
                        default="primary")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5001)
    parser.add_argument("--replica-url", default="http://127.0.0.1:5002",
                        help="адрес реплики (только для основного узла)")
    parser.add_argument("--replica-delay", type=float, default=5.0,
                        help="задержка доставки в режиме async, с")
    parser.add_argument("--replica-timeout", type=float, default=5.0,
                        help="таймаут запроса к реплике, с")
    parser.add_argument("--ack-delay", type=float, default=0.0,
                        help="задержка ответа реплики, с — имитация сетевой "
                             "задержки между узлами")
    parser.add_argument("--read-delay", type=float, default=0.0,
                        help="задержка ответа на чтение, с — имитация "
                             "перегруженного узла")
    parser.add_argument("--sync-from", default=None,
                        help="адрес узла, с которого забрать состояние при "
                             "старте (восстановление после перезапуска)")
    parser.add_argument("--log-file", default=None)
    args = parser.parse_args()

    ROLE = args.role
    REPLICA_URL = args.replica_url.rstrip("/")
    REPLICA_DELAY = args.replica_delay
    REPLICA_TIMEOUT = args.replica_timeout
    ACK_DELAY = args.ack_delay
    READ_DELAY = args.read_delay

    setup_logging(ROLE, args.log_file)
    seed()
    log.info("Узел %s запущен на %s:%s, начальное состояние: "
             "Laptop, price=500000, stock=10, version=1",
             ROLE, args.host, args.port)
    if ROLE == "primary":
        log.info("Реплика: %s, задержка async-доставки %.0f с",
                 REPLICA_URL, REPLICA_DELAY)
    if args.sync_from:
        catch_up_from(args.sync_from)

    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
