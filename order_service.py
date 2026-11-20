#!/usr/bin/env python3
"""
Лабораторная работа 5 — сервис заказов с переключением на реплику.

Читает товар сначала у основного каталога с коротким таймаутом (1 с).
Если основной узел не ответил — не запущен, перегружен или просто
медленный, — повторяет чтение у реплики и оформляет заказ по её данным.

Запуск:
    python3 order_service.py --port 5003 \\
            --primary-url http://127.0.0.1:5001 \\
            --replica-url http://127.0.0.1:5002 \\
            --timeout 1.0
"""

from __future__ import annotations

import argparse
import logging
import sys
import threading

import requests
from flask import Flask, jsonify, request

app = Flask(__name__)
app.json.ensure_ascii = False
app.json.sort_keys = False

PRIMARY_URL = "http://127.0.0.1:5001"
REPLICA_URL = "http://127.0.0.1:5002"
PRIMARY_TIMEOUT = 1.0      # таймаут обращения к основному узлу, с
REPLICA_TIMEOUT = 2.0      # таймаут обращения к реплике, с

_orders: dict[int, dict] = {}
_next_id = 101             # нумерация заказов начинается со 101
_lock = threading.Lock()

log = logging.getLogger("orders")


class NodeUnavailable(Exception):
    """Узел каталога не ответил: не запущен, недоступен или слишком медленный."""


class ProductNotFound(Exception):
    """Узел ответил, но товара с таким id нет."""


def read_from(url: str, product_id: int, timeout: float) -> dict:
    """
    Чтение товара с конкретного узла каталога.

    Таймаут обязателен: без него при зависшем узле поток сервиса заказов
    ждал бы ответа неопределённо долго и переключение никогда бы не
    произошло.
    """
    try:
        response = requests.get(f"{url}/products/{product_id}", timeout=timeout)
    except requests.exceptions.ConnectionError:
        raise NodeUnavailable("нет соединения (сервис не запущен)")
    except requests.exceptions.Timeout:
        raise NodeUnavailable(f"нет ответа за {timeout} с (таймаут)")
    except requests.exceptions.RequestException as exc:
        raise NodeUnavailable(f"ошибка запроса: {exc.__class__.__name__}")

    if response.status_code == 404:
        raise ProductNotFound(f"товар с id={product_id} отсутствует в каталоге")
    if response.status_code != 200:
        raise NodeUnavailable(f"неожиданный код ответа {response.status_code}")
    return response.json()


def read_product(product_id: int) -> tuple[dict, str, str | None]:
    """
    Чтение с переключением: сначала основной узел, при неудаче — реплика.

    Возвращает (товар, источник, причина переключения). Причина равна None,
    если ответил основной узел.
    """
    try:
        product = read_from(PRIMARY_URL, product_id, PRIMARY_TIMEOUT)
        log.info("Чтение товара id=%s: ответил PRIMARY, version=%s",
                 product_id, product.get("version"))
        return product, "primary", None
    except ProductNotFound:
        # Основной узел доступен и ответил определённо: товара нет.
        # Это ошибка запроса, а не отказ узла — переключаться не нужно.
        raise
    except NodeUnavailable as exc:
        reason = str(exc)
        log.warning("PRIMARY не ответил (%s) — перехожу на РЕПЛИКУ", reason)

    product = read_from(REPLICA_URL, product_id, REPLICA_TIMEOUT)
    log.info("Чтение товара id=%s: ответила REPLICA, version=%s "
             "(данные могут быть устаревшими)",
             product_id, product.get("version"))
    return product, "replica", reason


@app.errorhandler(404)
def handle_404(error):
    return jsonify({"error": "not_found",
                    "message": "Запрошенный ресурс не найден"}), 404


@app.errorhandler(405)
def handle_405(error):
    return jsonify({"error": "method_not_allowed",
                    "message": "Метод не поддерживается для этого адреса"}), 405


@app.post("/orders")
def create_order():
    """
    POST /orders — создать заказ.

    Тело: {"product_id": 1, "quantity": 2}
    Необязательное поле "catalog_source" со значением "primary" или
    "replica" заставляет читать конкретный узел без переключения —
    используется для демонстраций из ЛР 4.
    """
    global _next_id

    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        return jsonify({"error": "bad_request",
                        "message": "Ожидается тело запроса в формате JSON"}), 400

    product_id = data.get("product_id")
    if not isinstance(product_id, int) or isinstance(product_id, bool):
        return jsonify({"error": "validation_error",
                        "message": "Поле 'product_id' должно быть целым числом"}), 400

    quantity = data.get("quantity")
    if not isinstance(quantity, int) or isinstance(quantity, bool) or quantity < 1:
        return jsonify({"error": "validation_error",
                        "message": "Поле 'quantity' должно быть целым числом ≥ 1"}), 400

    forced = data.get("catalog_source")
    if forced is not None and str(forced).lower() not in ("primary", "replica"):
        return jsonify({
            "error": "validation_error",
            "message": "Поле 'catalog_source' должно быть 'primary' или 'replica'",
        }), 400

    try:
        if forced is None:
            product, source, reason = read_product(product_id)
        else:
            source = str(forced).lower()
            url = PRIMARY_URL if source == "primary" else REPLICA_URL
            timeout = PRIMARY_TIMEOUT if source == "primary" else REPLICA_TIMEOUT
            product = read_from(url, product_id, timeout)
            reason = None
    except NodeUnavailable as exc:
        # Заказ не создаётся только если недоступны оба узла.
        log.error("Заказ не создан: каталог недоступен (%s)", exc)
        return jsonify({
            "error": "catalog_unavailable",
            "message": f"Каталог недоступен: {exc}",
        }), 503
    except ProductNotFound as exc:
        return jsonify({"error": "product_not_found", "message": str(exc)}), 404

    price = product.get("price")
    name = product.get("name")
    if not isinstance(price, (int, float)) or not isinstance(name, str):
        return jsonify({"error": "catalog_bad_response",
                        "message": "Каталог вернул запись без имени или цены"}), 502

    with _lock:
        order = {
            "order_id": _next_id,
            "product_id": product_id,
            "name": name,
            "price": price,
            "stock": product.get("stock"),
            "quantity": quantity,
            "total": price * quantity,
            "catalog_source": source,
            "catalog_version": product.get("version"),
        }
        if reason:
            order["fallback_reason"] = reason
        _orders[_next_id] = order
        _next_id += 1

    log.info("Заказ order_id=%s создан: источник %s, version=%s, price=%s%s",
             order["order_id"], source, order["catalog_version"], price,
             f", причина переключения: {reason}" if reason else "")
    return jsonify(order), 201, {"Location": f"/orders/{order['order_id']}"}


@app.get("/orders/<int:order_id>")
def get_order(order_id: int):
    """GET /orders/<id> — сохранённый заказ; к каталогу обращения нет."""
    with _lock:
        order = _orders.get(order_id)
    if order is None:
        return jsonify({"error": "not_found",
                        "message": f"Заказ с order_id={order_id} не найден"}), 404
    return jsonify(order), 200


@app.get("/orders")
def list_orders():
    with _lock:
        items = [_orders[k] for k in sorted(_orders)]
    return jsonify({"count": len(items), "items": items}), 200


@app.get("/catalog-status")
def catalog_status():
    """
    GET /catalog-status — опрос /health обоих узлов каталога.

    Показывает, какой узел отвечает и какие версии товаров на нём лежат.
    """
    result = {}
    for name, url, timeout in (("primary", PRIMARY_URL, PRIMARY_TIMEOUT),
                               ("replica", REPLICA_URL, REPLICA_TIMEOUT)):
        try:
            response = requests.get(f"{url}/health", timeout=timeout)
            result[name] = {"reachable": response.status_code == 200,
                            "health": response.json()}
        except requests.exceptions.RequestException as exc:
            result[name] = {"reachable": False,
                            "error": exc.__class__.__name__}
    return jsonify(result), 200


@app.get("/health")
def health():
    return jsonify({"service": "orders", "status": "ok",
                    "primary_url": PRIMARY_URL,
                    "replica_url": REPLICA_URL,
                    "primary_timeout": PRIMARY_TIMEOUT,
                    "replica_timeout": REPLICA_TIMEOUT}), 200


def main() -> None:
    global PRIMARY_URL, REPLICA_URL, PRIMARY_TIMEOUT, REPLICA_TIMEOUT

    parser = argparse.ArgumentParser(description="Сервис заказов с fallback")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=5003)
    parser.add_argument("--primary-url", default="http://127.0.0.1:5001")
    parser.add_argument("--replica-url", default="http://127.0.0.1:5002")
    parser.add_argument("--timeout", type=float, default=1.0,
                        help="таймаут обращения к основному узлу, с")
    parser.add_argument("--replica-timeout", type=float, default=2.0,
                        help="таймаут обращения к реплике, с")
    parser.add_argument("--log-file", default=None)
    args = parser.parse_args()

    PRIMARY_URL = args.primary_url.rstrip("/")
    REPLICA_URL = args.replica_url.rstrip("/")
    PRIMARY_TIMEOUT = args.timeout
    REPLICA_TIMEOUT = args.replica_timeout

    handlers: list[logging.Handler] = [logging.StreamHandler(sys.stdout)]
    if args.log_file:
        handlers.append(logging.FileHandler(args.log_file, encoding="utf-8"))
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s [ORDERS ] %(message)s",
                        datefmt="%H:%M:%S", handlers=handlers)
    logging.getLogger("werkzeug").setLevel(logging.WARNING)

    log.info("Сервис заказов запущен на %s:%s", args.host, args.port)
    log.info("Основной узел: %s (таймаут %.1f с), реплика: %s (таймаут %.1f с)",
             PRIMARY_URL, PRIMARY_TIMEOUT, REPLICA_URL, REPLICA_TIMEOUT)

    app.run(host=args.host, port=args.port, threaded=True)


if __name__ == "__main__":
    main()
