"""Two small HTTP proxies for tests: FaultProxy damages traffic to the broker, Recorder logs what passes."""

import hashlib
import random

import aiohttp
from aiohttp import web

HOP = {"host", "content-length", "connection", "transfer-encoding", "keep-alive", "accept-encoding", "content-encoding"}


class Proxy:
    """Base: forwards every request to `target` and returns the answer. Subclasses override `handle`."""

    def __init__(self, target: str):
        self.target, self.port, self._runner, self._http = target.rstrip("/"), 0, None, None

    async def start(self) -> int:
        self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None), auto_decompress=False)
        app = web.Application(client_max_size=1 << 30)
        app.router.add_route("*", "/{tail:.*}", self.handle)
        self._runner = web.AppRunner(app, access_log=None, shutdown_timeout=1)
        await self._runner.setup()
        site = web.TCPSite(self._runner, "127.0.0.1", 0)
        await site.start()
        self.port = site._server.sockets[0].getsockname()[1]
        return self.port

    async def stop(self) -> None:
        await self._runner.cleanup()
        await self._http.close()

    async def forward(self, request: web.Request, body: bytes) -> tuple[int, dict, bytes]:
        headers = {k: v for k, v in request.headers.items() if k.lower() not in HOP}
        async with self._http.request(request.method, self.target + request.path_qs, headers=headers, data=body) as r:
            return r.status, {k: v for k, v in r.headers.items() if k.lower() not in HOP}, await r.read()

    async def handle(self, request: web.Request) -> web.StreamResponse:
        status, headers, body = await self.forward(request, await request.read())
        return web.Response(status=status, headers=headers, body=body)


class Recorder(Proxy):
    """Logs every request. `seen` has (path, sha1 of the body) on arrival; `log` adds the status when the
    answer is back (an unreachable target is 502)."""

    def __init__(self, target: str, lose_first: dict[str, int] | None = None):
        super().__init__(target)
        self.seen: list[tuple[str, str]] = []
        self.log: list[tuple[str, str, int]] = []
        self.lose = dict(lose_first or {})  # path -> how many answers to cut: the target ran it, the caller must retry

    async def handle(self, request: web.Request) -> web.StreamResponse:
        body = await request.read()
        self.seen.append((request.path, hashlib.sha1(body).hexdigest()))
        try:
            status, headers, rbody = await self.forward(request, body)
        except aiohttp.ClientError:
            status, headers, rbody = 502, {}, b""
        self.log.append((request.path, hashlib.sha1(body).hexdigest(), status))
        if self.lose.get(request.path, 0) > 0 and status == 200:
            self.lose[request.path] -= 1
            request.transport.abort()
        return web.Response(status=status, headers=headers, body=rbody)


class FaultProxy(Proxy):
    """In front of the broker. Per request, with the given probabilities (seeded, so a run can be repeated):
       dup_send:  a `send` reaches the broker twice (a repeat the sender could not know was needed)
       lose_send: the broker accepts a `send` but the answer is lost: the connection is cut
       lose_recv: the broker hands frames to a `recv` but the answer is lost (the frames stay unacked)
       lose_ack:  the broker takes an `ack` but the answer is lost"""

    def __init__(self, target: str, seed: int = 1, dup_send=0.05, lose_send=0.05, lose_recv=0.05, lose_ack=0.05):
        super().__init__(target)
        self.rng, self.p = random.Random(seed), {"dup_send": dup_send, "lose_send": lose_send,
                                                  "lose_recv": lose_recv, "lose_ack": lose_ack}
        self.count = {k: 0 for k in self.p}

    def hit(self, fault: str) -> bool:
        if self.rng.random() < self.p[fault]:
            self.count[fault] += 1
            return True
        return False

    async def handle(self, request: web.Request) -> web.StreamResponse:
        body = await request.read()
        kind = "send" if "/send/" in request.path else request.path.rsplit("/", 1)[-1]
        if kind == "send" and self.hit("dup_send"):
            await self.forward(request, body)
        status, headers, rbody = await self.forward(request, body)
        if (kind == "send" and status < 300 and self.hit("lose_send")) or \
           (kind == "recv" and status == 200 and self.hit("lose_recv")) or \
           (kind == "ack" and status < 300 and self.hit("lose_ack")):
            request.transport.abort()  # the broker did the work; the caller never learns
            return web.Response(status=502)
        return web.Response(status=status, headers=headers, body=rbody)
