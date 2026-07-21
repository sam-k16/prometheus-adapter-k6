import time
import random
import uuid
import structlog

from fastapi import FastAPI, HTTPException, Request
from pydantic import BaseModel
from prometheus_client import (
    Counter,
    Histogram,
    Gauge,
    generate_latest,
    CONTENT_TYPE_LATEST,
)
from fastapi.responses import Response


# ── Structured logging ────────────────────────────────────────────

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

log = structlog.get_logger().bind(service="inventory-service")


app = FastAPI(title="Inventory Service")


# ── Prometheus metrics ────────────────────────────────────────────

REQUEST_COUNT = Counter(
    "inventory_requests_total",
    "Total HTTP requests to Inventory Service",
    ["method", "endpoint", "status"],
)

# Pre-initialize the metric series used by the HPA.
REQUEST_COUNT.labels(
    method="PUT",
    endpoint="/inventory/reduce",
    status="200",
).inc(0)

REQUEST_LATENCY = Histogram(
    "inventory_request_duration_seconds",
    "Request latency for Inventory Service",
    ["endpoint"],
    buckets=[0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0],
)

STOCK_LEVEL = Gauge(
    "inventory_stock_level",
    "Current stock level per product",
    ["product_id"],
)

LOW_STOCK_ALERTS = Counter(
    "inventory_low_stock_events_total",
    "Number of times a product hit low stock threshold",
    ["product_id"],
)

STOCK_REDUCTION_FAILURES = Counter(
    "inventory_reduction_failures_total",
    "Failed stock reduction attempts",
    ["product_id"],
)


# ── In-memory inventory ───────────────────────────────────────────
# Demo only. A production application would use shared persistent
# storage/database.

inventory: dict = {
    # High initial stock prevents demo load tests from exhausting
    # per-pod in-memory inventory when HPA creates new replicas.
    "PROD-001": 100000,
    "PROD-002": 50000,
    "PROD-003": 200000,
    "PROD-004": 20000,
    "PROD-005": 150000,
}

LOW_STOCK_THRESHOLD = 10


for pid, qty in inventory.items():
    STOCK_LEVEL.labels(
        product_id=pid
    ).set(qty)


# ── Models ────────────────────────────────────────────────────────

class StockUpdateRequest(BaseModel):
    product_id: str
    quantity: int


# ── Metric helper ─────────────────────────────────────────────────
# Converts:
#
# /inventory/PROD-001
# /inventory/PROD-002
#
# into:
#
# /inventory/{product_id}
#
# This prevents high-cardinality endpoint labels.

def get_route_template(request: Request) -> str:

    route = request.scope.get("route")

    if route is not None:
        return route.path

    return request.url.path


# ── Observability middleware ──────────────────────────────────────

@app.middleware("http")
async def observability_middleware(
    request: Request,
    call_next,
):

    # Prometheus scraping should not count as application traffic.
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

    try:

        response = await call_next(request)

        return response

    finally:

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


# ── Kubernetes health endpoints ───────────────────────────────────

@app.get("/health/live")
def liveness():

    return {
        "status": "alive",
        "service": "inventory-service",
    }


@app.get("/health/ready")
def readiness():

    return {
        "status": "ready",
        "service": "inventory-service",
    }


# ── Prometheus metrics endpoint ───────────────────────────────────

@app.get("/metrics")
def metrics():

    return Response(
        generate_latest(),
        media_type=CONTENT_TYPE_LATEST,
    )


# ── Inventory routes ──────────────────────────────────────────────

@app.get("/inventory")
def list_inventory():

    log.info(
        "listing_inventory",
        products=len(inventory),
    )

    return {
        "inventory": inventory
    }


@app.get("/inventory/{product_id}")
def get_stock(
    product_id: str,
    request: Request,
):

    trace_id = request.headers.get(
        "x-trace-id",
        str(uuid.uuid4())[:16],
    )

    if product_id not in inventory:

        log.warning(
            "product_not_found",
            product_id=product_id,
            trace_id=trace_id,
        )

        raise HTTPException(
            status_code=404,
            detail="Product not found",
        )

    return {
        "product_id": product_id,
        "stock": inventory[product_id],
    }


@app.put("/inventory/reduce")
def reduce_stock(
    req: StockUpdateRequest,
    request: Request,
):

    trace_id = request.headers.get(
        "x-trace-id",
        str(uuid.uuid4())[:16],
    )

    inv_log = log.bind(
        trace_id=trace_id,
        product_id=req.product_id,
        quantity=req.quantity,
    )

    # Simulated slow database response.
    #
    # This endpoint is synchronous (def), so FastAPI executes it
    # through its thread pool. time.sleep() is acceptable here
    # for the simulated blocking operation.

    if random.random() < 0.08:

        delay = random.uniform(0.8, 2.0)

        inv_log.warning(
            "slow_stock_check",
            delay_seconds=round(delay, 2),
        )

        time.sleep(delay)

    pid = req.product_id

    if pid not in inventory:

        inv_log.error(
            "product_not_found_for_reduction"
        )

        raise HTTPException(
            status_code=404,
            detail="Product not found",
        )

    if inventory[pid] < req.quantity:

        STOCK_REDUCTION_FAILURES.labels(
            product_id=pid
        ).inc()

        inv_log.error(
            "insufficient_stock",
            available=inventory[pid],
            requested=req.quantity,
        )

        raise HTTPException(
            status_code=400,
            detail=f"Insufficient stock for {pid}",
        )

    old_stock = inventory[pid]

    inventory[pid] -= req.quantity

    STOCK_LEVEL.labels(
        product_id=pid
    ).set(inventory[pid])

    inv_log.info(
        "stock_reduced",
        old_stock=old_stock,
        new_stock=inventory[pid],
    )

    if inventory[pid] < LOW_STOCK_THRESHOLD:

        LOW_STOCK_ALERTS.labels(
            product_id=pid
        ).inc()

        inv_log.warning(
            "low_stock_warning",
            remaining=inventory[pid],
            threshold=LOW_STOCK_THRESHOLD,
        )

    if inventory[pid] == 0:

        inv_log.error(
            "product_out_of_stock"
        )

    return {
        "product_id": pid,
        "remaining_stock": inventory[pid],
    }


@app.put("/inventory/restock")
def restock(
    req: StockUpdateRequest,
    request: Request,
):

    trace_id = request.headers.get(
        "x-trace-id",
        str(uuid.uuid4())[:16],
    )

    pid = req.product_id

    if pid not in inventory:
        inventory[pid] = 0

    inventory[pid] += req.quantity

    STOCK_LEVEL.labels(
        product_id=pid
    ).set(inventory[pid])

    log.info(
        "product_restocked",
        product_id=pid,
        new_stock=inventory[pid],
        trace_id=trace_id,
    )

    return {
        "product_id": pid,
        "new_stock": inventory[pid],
    }