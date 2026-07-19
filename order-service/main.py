import asyncio
import os
import random
import time
import uuid
from contextlib import asynccontextmanager

import httpx
import structlog
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import Response
from prometheus_client import (
    CONTENT_TYPE_LATEST,
    Counter,
    Gauge,
    Histogram,
    generate_latest,
)
from pydantic import BaseModel


# ─────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────

INVENTORY_SERVICE_URL = os.getenv(
    "INVENTORY_SERVICE_URL",
    "http://inventory-service:8001",
)

NOTIFICATION_SERVICE_URL = os.getenv(
    "NOTIFICATION_SERVICE_URL",
    "http://notification-service:8002",
)


# ─────────────────────────────────────────────────────────────────
# Structured logging
# ─────────────────────────────────────────────────────────────────

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(10),
    context_class=dict,
    logger_factory=structlog.PrintLoggerFactory(),
)

log = structlog.get_logger().bind(service="order-service")


# ─────────────────────────────────────────────────────────────────
# Prometheus metrics
# ─────────────────────────────────────────────────────────────────

REQUEST_COUNT = Counter(
    "order_requests_total",
    "Total HTTP requests to Order Service",
    ["method", "endpoint", "status"],
)

REQUEST_LATENCY = Histogram(
    "order_request_duration_seconds",
    "Request latency for Order Service",
    ["endpoint"],
    buckets=[
        0.01,
        0.05,
        0.1,
        0.25,
        0.5,
        1.0,
        2.5,
        5.0,
    ],
)

ORDERS_CREATED = Counter(
    "orders_created_total",
    "Total number of orders created",
    ["status"],
)

ACTIVE_ORDERS = Gauge(
    "active_orders_count",
    "Number of currently active orders",
)

INVENTORY_CALL_ERRORS = Counter(
    "order_inventory_call_errors_total",
    "Failed calls from Order Service to Inventory Service",
)

ACTIVE_REQUESTS = Gauge(
    "order_active_requests",
    "Current number of active requests",
)


# ─────────────────────────────────────────────────────────────────
# In-memory store
#
# Demo only:
# Production applications should use a shared persistent datastore.
# ─────────────────────────────────────────────────────────────────

orders: dict = {}


# ─────────────────────────────────────────────────────────────────
# HTTP client lifecycle
#
# One AsyncClient is created when the application starts and reused
# for downstream requests. This enables connection pooling.
# ─────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):

    log.info("application_starting")

    timeout = httpx.Timeout(
        connect=2.0,
        read=3.0,
        write=3.0,
        pool=2.0,
    )

    limits = httpx.Limits(
        max_connections=100,
        max_keepalive_connections=20,
    )

    app.state.http_client = httpx.AsyncClient(
        timeout=timeout,
        limits=limits,
    )

    log.info("http_client_initialized")

    try:
        yield

    finally:
        await app.state.http_client.aclose()
        log.info("application_shutdown")


app = FastAPI(
    title="Order Service",
    lifespan=lifespan,
)


# ─────────────────────────────────────────────────────────────────
# Models
# ─────────────────────────────────────────────────────────────────

class OrderRequest(BaseModel):
    product_id: str
    quantity: int
    customer_id: str


# ─────────────────────────────────────────────────────────────────
# Metric helper
#
# Returns route templates such as:
#
# /orders/{order_id}
#
# instead of:
#
# /orders/abc123
# /orders/xyz456
#
# This prevents high-cardinality Prometheus labels.
# ─────────────────────────────────────────────────────────────────

def get_route_template(request: Request) -> str:

    route = request.scope.get("route")

    if route is not None:
        return route.path

    return request.url.path


# ─────────────────────────────────────────────────────────────────
# Observability middleware
# ─────────────────────────────────────────────────────────────────

@app.middleware("http")
async def observability_middleware(
    request: Request,
    call_next,
):

    # Do not count Prometheus scraping as application traffic.
    if request.url.path == "/metrics":
        return await call_next(request)

    trace_id = request.headers.get(
        "x-trace-id",
        str(uuid.uuid4())[:16],
    )

    start = time.perf_counter()

    req_log = log.bind(
        trace_id=trace_id,
        method=request.method,
        path=request.url.path,
    )

    req_log.info("request_started")

    response = None

    ACTIVE_REQUESTS.inc()

    try:

        response = await call_next(request)

        return response

    finally:

        # Always decrement, including when an unhandled
        # exception occurs.
        ACTIVE_REQUESTS.dec()

        duration = time.perf_counter() - start

        endpoint = get_route_template(request)

        status = (
            response.status_code
            if response is not None
            else 500
        )

        req_log.info(
            "request_completed",
            status=status,
            duration_ms=round(duration * 1000, 2),
        )

        REQUEST_COUNT.labels(
            method=request.method,
            endpoint=endpoint,
            status=str(status),
        ).inc()

        REQUEST_LATENCY.labels(
            endpoint=endpoint,
        ).observe(duration)

        if response is not None:
            response.headers["x-trace-id"] = trace_id


# ─────────────────────────────────────────────────────────────────
# Kubernetes health endpoints
# ─────────────────────────────────────────────────────────────────

@app.get("/health/live")
def liveness():

    return {
        "status": "alive",
        "service": "order-service",
    }


@app.get("/health/ready")
def readiness():

    # Keep readiness local to this service.
    #
    # Do not fail readiness simply because Inventory or Notification
    # temporarily becomes unavailable. Doing that can create
    # cascading failures.

    return {
        "status": "ready",
        "service": "order-service",
    }


# ─────────────────────────────────────────────────────────────────
# Prometheus metrics endpoint
# ─────────────────────────────────────────────────────────────────

@app.get("/metrics")
def metrics():

    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# ─────────────────────────────────────────────────────────────────
# Create order
# ─────────────────────────────────────────────────────────────────

@app.post("/orders")
async def create_order(
    order: OrderRequest,
    request: Request,
):

    trace_id = request.headers.get(
        "x-trace-id",
        str(uuid.uuid4())[:16],
    )

    order_log = log.bind(
        trace_id=trace_id,
        product_id=order.product_id,
        customer_id=order.customer_id,
        quantity=order.quantity,
    )

    order_log.info("order_creation_started")

    # Simulate occasional slow application processing.
    #
    # asyncio.sleep() is used instead of time.sleep()
    # because this is an async endpoint.
    if random.random() < 0.1:

        delay = random.uniform(1.5, 3.0)

        order_log.warning(
            "simulated_slow_response",
            delay_seconds=round(delay, 2),
        )

        await asyncio.sleep(delay)

    # Simulate occasional internal failures.
    if random.random() < 0.05:

        order_log.error(
            "order_creation_failed",
            reason="simulated_internal_error",
        )

        ORDERS_CREATED.labels(
            status="failed",
        ).inc()

        raise HTTPException(
            status_code=500,
            detail="Internal error processing order",
        )

    order_id = str(uuid.uuid4())[:8]

    client: httpx.AsyncClient = request.app.state.http_client


    # ─────────────────────────────────────────────────────────────
    # Inventory service call
    # ─────────────────────────────────────────────────────────────

    try:

        order_log.info(
            "calling_inventory_service",
            inventory_url=INVENTORY_SERVICE_URL,
        )

        inv_resp = await client.put(
            f"{INVENTORY_SERVICE_URL}/inventory/reduce",
            json={
                "product_id": order.product_id,
                "quantity": order.quantity,
            },
            headers={
                "x-trace-id": trace_id,
            },
        )

        if inv_resp.status_code != 200:

            INVENTORY_CALL_ERRORS.inc()

            ORDERS_CREATED.labels(
                status="failed",
            ).inc()

            order_log.error(
                "inventory_check_failed",
                status_code=inv_resp.status_code,
                response=inv_resp.text,
            )

            raise HTTPException(
                status_code=400,
                detail="Insufficient inventory",
            )

        order_log.info(
            "inventory_check_passed",
        )

    except httpx.RequestError as exc:

        INVENTORY_CALL_ERRORS.inc()

        ORDERS_CREATED.labels(
            status="failed",
        ).inc()

        order_log.error(
            "inventory_service_unreachable",
            error=str(exc),
        )

        raise HTTPException(
            status_code=503,
            detail="Inventory service unavailable",
        )


    # ─────────────────────────────────────────────────────────────
    # Store order
    #
    # In-memory storage is intentionally used for this project.
    # A real production application would use a shared database.
    # ─────────────────────────────────────────────────────────────

    orders[order_id] = {
        "order_id": order_id,
        "product_id": order.product_id,
        "quantity": order.quantity,
        "customer_id": order.customer_id,
        "status": "confirmed",
    }

    ACTIVE_ORDERS.set(
        len(orders),
    )

    ORDERS_CREATED.labels(
        status="success",
    ).inc()

    order_log.info(
        "order_created_successfully",
        order_id=order_id,
    )


    # ─────────────────────────────────────────────────────────────
    # Notification service call
    #
    # Notification failure does not fail the order because the
    # order itself has already been successfully created.
    # ─────────────────────────────────────────────────────────────

    try:

        notification_response = await client.post(
            f"{NOTIFICATION_SERVICE_URL}/notify",
            json={
                "customer_id": order.customer_id,
                "message": f"Order {order_id} confirmed!",
            },
            headers={
                "x-trace-id": trace_id,
            },
        )

        if notification_response.is_success:

            order_log.info(
                "notification_sent",
                order_id=order_id,
            )

        else:

            order_log.warning(
                "notification_failed",
                order_id=order_id,
                status_code=notification_response.status_code,
            )

    except httpx.RequestError as exc:

        order_log.warning(
            "notification_service_unreachable",
            error=str(exc),
            order_id=order_id,
        )

    return orders[order_id]


# ─────────────────────────────────────────────────────────────────
# List orders
# ─────────────────────────────────────────────────────────────────

@app.get("/orders")
def list_orders():

    log.info(
        "listing_orders",
        total=len(orders),
    )

    return {
        "orders": list(orders.values()),
        "total": len(orders),
    }


# ─────────────────────────────────────────────────────────────────
# Get order
# ─────────────────────────────────────────────────────────────────

@app.get("/orders/{order_id}")
def get_order(order_id: str):

    if order_id not in orders:

        log.warning(
            "order_not_found",
            order_id=order_id,
        )

        raise HTTPException(
            status_code=404,
            detail="Order not found",
        )

    return orders[order_id]

