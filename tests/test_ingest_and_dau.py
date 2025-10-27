
from datetime import datetime, timezone
import pytest

@pytest.mark.asyncio
async def test_ingest_and_dau(client):
    payload = [
        {
            "event_id": "22222222-2222-2222-2222-222222222221",
            "occurred_at": datetime(2025, 8, 4, 8, 0, tzinfo=timezone.utc).isoformat(),
            "user_id": "u1",
            "event_type": "signup",
            "properties": {},
        },
        {
            "event_id": "22222222-2222-2222-2222-222222222222",
            "occurred_at": datetime(2025, 8, 4, 9, 0, tzinfo=timezone.utc).isoformat(),
            "user_id": "u2",
            "event_type": "signin",
            "properties": {},
        },
    ]
    r = await client.post("/events", json=payload)
    assert r.status_code in (200, 201)

    res = await client.get("/stats/dau", params={"from": "2025-08-04", "to": "2025-08-04"})
    assert res.status_code == 200
    assert res.json() == [{"date": "2025-08-04", "dau": 2}]
