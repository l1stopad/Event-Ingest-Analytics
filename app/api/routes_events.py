from datetime import datetime, timezone
from typing import List, Dict, Any
from uuid import UUID

from fastapi import APIRouter, HTTPException, Response
from pydantic import BaseModel, Field, model_validator
from psycopg.types.json import Json
from prometheus_client import Counter, Histogram  # +++

from ..infrastructure.db import get_conn

router = APIRouter()

# ----- Prometheus metrics -----
INGEST_EVENTS = Counter(
    "ingest_events_total",
    "Total events received by POST /events",
    ["result"],  # "inserted", "duplicate", "error"
)
INGEST_BATCH = Histogram(
    "ingest_batch_seconds",
    "Latency of ingest batch processing",
    buckets=(0.01, 0.05, 0.1, 0.25, 0.5, 1, 2, 5),
)

class EventIn(BaseModel):
    event_id: UUID
    occurred_at: datetime
    user_id: str = Field(min_length=1, max_length=256)
    event_type: str = Field(min_length=1, max_length=128)
    properties: Dict[str, Any] = Field(default_factory=dict)

    @model_validator(mode="after")
    def ensure_tz(self) -> "EventIn":
        # Якщо прийшла "naive" дата — вважаємо її UTC
        if self.occurred_at.tzinfo is None:
            self.occurred_at = self.occurred_at.replace(tzinfo=timezone.utc)
        return self

class IngestResult(BaseModel):
    ingested: int
    duplicates: int

@router.post("/events", response_model=IngestResult, summary="Batch ingest events (idempotent)")
async def ingest_events(events: List[EventIn], response: Response):
    if not events:
        raise HTTPException(status_code=400, detail="Empty payload")
    if len(events) > 10_000:
        raise HTTPException(status_code=413, detail="Too many events in a single batch (max 10k)")

    conn = await get_conn()

    inserted = 0
    sql = """
        INSERT INTO events (event_id, occurred_at, user_id, event_type, properties)
        VALUES (%(event_id)s, %(occurred_at)s, %(user_id)s, %(event_type)s, %(properties)s)
        ON CONFLICT (event_id) DO NOTHING;
    """

    with INGEST_BATCH.time():  # вимірюємо час батчу
        async with conn.cursor() as cur:
            for e in events:
                try:
                    params = {
                        "event_id": e.event_id,
                        "occurred_at": e.occurred_at,
                        "user_id": e.user_id,
                        "event_type": e.event_type,
                        "properties": Json(e.properties),
                    }
                    await cur.execute(sql, params)
                    if cur.rowcount and cur.rowcount > 0:
                        inserted += 1
                        INGEST_EVENTS.labels("inserted").inc()
                    else:
                        INGEST_EVENTS.labels("duplicate").inc()
                except Exception:
                    INGEST_EVENTS.labels("error").inc()
                    raise

    
    if inserted == len(events):
        response.status_code = 201
    else:
        response.status_code = 200

    return IngestResult(ingested=inserted, duplicates=len(events) - inserted)
