import os
import uuid
from datetime import datetime, timezone, timedelta

import pytest
from httpx import AsyncClient
from app.main import app

@pytest.mark.asyncio
async def test_ingest_and_dau(monkeypatch):
    # для локального запуску тестів (змінити хост БД, якщо треба)
    monkeypatch.setenv("POSTGRES_HOST", os.getenv("POSTGRES_HOST", "localhost"))

    async with AsyncClient(app=app, base_url="http://test") as ac:
        # health
        r = await ac.get("/health")
        assert r.status_code == 200

        today = datetime.now(timezone.utc).replace(hour=12, minute=0, second=0, microsecond=0)
        payload = [
            {
                "event_id": str(uuid.uuid4()),
                "occurred_at": today.isoformat(),
                "user_id": "u1",
                "event_type": "signin",
                "properties": {"country": "UA"},
            },
            {
                "event_id": str(uuid.uuid4()),
                "occurred_at": today.isoformat(),
                "user_id": "u2",
                "event_type": "purchase",
                "properties": {"amount": 10},
            },
        ]

        r = await ac.post("/events", json=payload)
        assert r.status_code in (200, 201)
        data = r.json()
        assert data["ingested"] == 2

        # повторний батч (ідемпотентність)
        r = await ac.post("/events", json=payload)
        assert r.status_code in (200, 201)
        data = r.json()
        assert data["duplicates"] == 2

        since = today.date().isoformat()
        r = await ac.get(f"/stats/dau?from={since}&to={since}")
        assert r.status_code == 200
        arr = r.json()
        assert len(arr) == 1
        assert arr[0]["dau"] == 2
