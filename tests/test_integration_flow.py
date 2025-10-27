
from datetime import datetime, timezone
import pytest

@pytest.mark.asyncio
async def test_flow_ingest_then_stats(client):
    # 1) інгест пачки (5 подій, 3 унікальних юзери в один день)
    items = []
    for i in range(5):
        items.append({
            "event_id": f"11111111-1111-1111-1111-11111111110{i}",
            "occurred_at": datetime(2025, 8, 3, 12, i, tzinfo=timezone.utc).isoformat(),
            "user_id": f"u{i%3}",
            "event_type": "view",
            "properties": {"page": f"p{i}"},
        })
    r = await client.post("/events", json=items)
    assert r.status_code in (200, 201)
    j = r.json()
    assert j["ingested"] == 5 and j["duplicates"] == 0

    # 2) DAU
    dau = await client.get("/stats/dau", params={"from": "2025-08-03", "to": "2025-08-03"})
    assert dau.status_code == 200
    assert dau.json() == [{"date": "2025-08-03", "dau": 3}]

    # 3) top-events
    top = await client.get("/stats/top-events", params={"from": "2025-08-03", "to": "2025-08-03"})
    assert top.status_code == 200
    rows = top.json()
    assert rows and rows[0]["event_type"] == "view" and rows[0]["count"] == 5
