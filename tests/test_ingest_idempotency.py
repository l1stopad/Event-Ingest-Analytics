
import uuid
from datetime import datetime, timezone
import pytest

@pytest.mark.asyncio
async def test_ingest_idempotency(client):
    payload = [
        {
            "event_id": str(uuid.uuid4()),
            "occurred_at": datetime(2025, 8, 1, 12, 0, tzinfo=timezone.utc).isoformat(),
            "user_id": "u1",
            "event_type": "signin",
            "properties": {"country": "UA"},
        }
    ]

    r1 = await client.post("/events", json=payload)
    assert r1.status_code in (200, 201)
    body1 = r1.json()
    assert body1["ingested"] == 1
    assert body1["duplicates"] == 0

    r2 = await client.post("/events", json=payload)
    assert r2.status_code in (200, 201)
    body2 = r2.json()
    assert body2["ingested"] == 0
    assert body2["duplicates"] == 1
