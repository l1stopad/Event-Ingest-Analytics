from psycopg.types.json import Json
from datetime import datetime
from typing import List, Dict, Any
from uuid import UUID


from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..infrastructure.db import get_conn

router = APIRouter()

class EventIn(BaseModel):
    event_id: UUID
    occurred_at: datetime
    user_id: str = Field(min_length=1, max_length=256)
    event_type: str = Field(min_length=1, max_length=128)
    properties: Dict[str, Any] = Field(default_factory=dict)

class IngestResult(BaseModel):
    ingested: int
    duplicates: int

@router.post("/events", response_model=IngestResult, summary="Batch ingest events (idempotent)")
async def ingest_events(events: List[EventIn]):
    if not events:
        raise HTTPException(status_code=400, detail="Empty payload")

    # Simple size guard (можна винести в settings)
    if len(events) > 10_000:
        raise HTTPException(status_code=413, detail="Too many events in a single batch (max 10k)")

    conn = await get_conn()

    inserted = 0
    sql = """
        INSERT INTO events (event_id, occurred_at, user_id, event_type, properties)
        VALUES (%(event_id)s, %(occurred_at)s, %(user_id)s, %(event_type)s, %(properties)s)
        ON CONFLICT (event_id) DO NOTHING;
    """

    async with conn.cursor() as cur:
        for e in events:
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

    return IngestResult(ingested=inserted, duplicates=len(events) - inserted)