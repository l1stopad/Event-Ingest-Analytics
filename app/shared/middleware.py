import time
from typing import Callable, Awaitable, Dict, Tuple
from uuid import uuid4

from starlette.types import ASGIApp, Receive, Scope, Send
from starlette.responses import JSONResponse
import structlog

from .settings import settings

log = structlog.get_logger()

# -------- Request ID --------
class RequestIDMiddleware:
    """Inject X-Request-ID and bind to structlog context."""

    def __init__(self, app: ASGIApp, header_name: str = "X-Request-ID") -> None:
        self.app = app
        self.header_name = header_name

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        # inbound headers
        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        req_id = headers.get(self.header_name.lower()) or str(uuid4())

        # bind to structlog
        structlog.contextvars.bind_contextvars(request_id=req_id)

        async def send_wrapper(message):
            if message["type"] == "http.response.start":
                headers_list = message.setdefault("headers", [])
                headers_list.append((self.header_name.encode(), req_id.encode()))
            return await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            structlog.contextvars.clear_contextvars()


# -------- Token-bucket Rate Limit --------
class TokenBucket:
    __slots__ = ("capacity", "refill_rate", "tokens", "last")

    def __init__(self, capacity: int, refill_rate: float) -> None:
        self.capacity = capacity
        self.refill_rate = refill_rate  # tokens per second
        self.tokens = float(capacity)
        self.last = time.perf_counter()

    def allow(self, cost: float = 1.0) -> bool:
        now = time.perf_counter()
        elapsed = now - self.last
        self.last = now
        self.tokens = min(self.capacity, self.tokens + elapsed * self.refill_rate)
        if self.tokens >= cost:
            self.tokens -= cost
            return True
        return False


class RateLimitMiddleware:
    """
    Simple per-key token-bucket. Key: X-API-Key or client IP.
    Use for write-heavy endpoints first.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        self.buckets: Dict[str, TokenBucket] = {}
        self.capacity = settings.rate_limit_burst
        self.refill = float(settings.rate_limit_rps)  # tokens/second

    def _key(self, scope: Scope) -> str:
        headers = {k.decode().lower(): v.decode() for k, v in scope.get("headers", [])}
        api_key = headers.get("x-api-key")
        if api_key:
            return f"key:{api_key}"
        client = scope.get("client")
        ip = (client[0] if client else "unknown")
        return f"ip:{ip}"

    def _bucket(self, key: str) -> TokenBucket:
        b = self.buckets.get(key)
        if b is None:
            b = TokenBucket(self.capacity, self.refill)
            self.buckets[key] = b
        return b

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return

        path = scope.get("path", "")
        method = scope.get("method", "GET")

        
        if method == "POST" and path == "/events":
            key = self._key(scope)
            bucket = self._bucket(key)
            if not bucket.allow():
                # 429 Too Many Requests
                await JSONResponse(
                    status_code=429,
                    content={"detail": "rate limit exceeded"},
                    headers={"Retry-After": "1"},
                )(scope, receive, send)
                return

        await self.app(scope, receive, send)
