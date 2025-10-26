from fastapi import FastAPI
from prometheus_fastapi_instrumentator import Instrumentator
import structlog

from .shared.logging import setup_logging
from .shared.settings import settings
from .api.routes_health import router as health_router
from .api.routes_events import router as events_router   # +++
from .api.routes_stats import router as stats_router     # +++
from .infrastructure.db import get_conn, ensure_migrations, shutdown

setup_logging()
log = structlog.get_logger()

app = FastAPI(title=settings.app_name)

# Routers
app.include_router(health_router, tags=["system"])
app.include_router(events_router, tags=["ingest"])  # +++
app.include_router(stats_router, tags=["stats"])    # +++

# Metrics
if settings.enable_metrics:
    Instrumentator().instrument(app).expose(app)

@app.on_event("startup")
async def on_startup():
    conn = await get_conn()
    await ensure_migrations()
    log.info("app_started", env=settings.env)

@app.on_event("shutdown")
async def on_shutdown():
    await shutdown()
    log.info("app_stopped")
