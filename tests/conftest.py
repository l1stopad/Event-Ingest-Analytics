
import pytest_asyncio
from httpx import AsyncClient, ASGITransport

from app.main import app
from app.infrastructure.db import get_conn


@pytest_asyncio.fixture(autouse=True)
async def _clean_db():
    """
    Перед кожним тестом чистимо таблицю events,
    щоб тести були детерміновані.
    """
    conn = await get_conn()
    async with conn.cursor() as cur:
        await cur.execute("TRUNCATE TABLE events;")
    yield


@pytest_asyncio.fixture
async def client():
    """
    HTTP-клієнт, який викликає FastAPI app напряму через ASGITransport
    (без реального мережевого порту).
    """
    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://test") as ac:
        yield ac
