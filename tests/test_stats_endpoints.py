
from datetime import datetime, timezone
import pytest

@pytest.mark.asyncio
async def test_dau_and_top_events(client):
    
    batch = [
        {
            "event_id": "00000000-0000-0000-0000-000000000001",
            "occurred_at": datetime(2025, 8, 1, 10, 0, tzinfo=timezone.utc).isoformat(),
            "user_id": "u1",
            "event_type": "signin",
            "properties": {"country": "UA"},
        },
        {
            "event_id": "00000000-0000-0000-0000-000000000002",
            "occurred_at": datetime(2025, 8, 1, 11, 0, tzinfo=timezone.utc).isoformat(),
            "user_id": "u2",
            "event_type": "purchase",
            "properties": {"country": "PL"},
        },
        {
            "event_id": "00000000-0000-0000-0000-000000000003",
            "occurred_at": datetime(2025, 8, 2, 9, 0, tzinfo=timezone.utc).isoformat(),
            "user_id": "u1",
            "event_type": "purchase",
            "properties": {"country": "UA"},
        },
    ]
    r = await client.post("/events", json=batch)
    assert r.status_code in (200, 201)

    dau = await client.get("/stats/dau", params={"from": "2025-08-01", "to": "2025-08-02"})
    assert dau.status_code == 200
    assert dau.json() == [
        {"date": "2025-08-01", "dau": 2},
        {"date": "2025-08-02", "dau": 1},
    ]

    top = await client.get("/stats/top-events", params={"from": "2025-08-01", "to": "2025-08-02", "limit": 10})
    assert top.status_code == 200
    rows = top.json()
    assert rows[0]["event_type"] == "purchase" and rows[0]["count"] == 2
    assert rows[1]["event_type"] == "signin" and rows[1]["count"] == 1

    dau_ua = await client.get("/stats/dau", params={
        "from": "2025-08-01", "to": "2025-08-02", "segment": "properties.country=UA"
    })
    assert dau_ua.status_code == 200
    assert dau_ua.json() == [
        {"date": "2025-08-01", "dau": 1},
        {"date": "2025-08-02", "dau": 1},
    ]
