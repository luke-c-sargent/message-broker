"""The broker's HTTP API (spec section 10): an aiohttp front for a BrokerCore.
Run it with: MASTER_SECRET=... python -m message_broker.broker"""

import asyncio
import dataclasses
import json
import logging
import os
import struct

from aiohttp import web

from ..errors import BrokerError
from ..types import SECONDS, MiB
from .core import BrokerCore, Limits

log = logging.getLogger("message_broker.broker")
STATUS = {"unauthorized": 401, "forbidden": 403, "session_gone": 410, "unknown_dest": 404, "conflict": 409,
          "queue_full": 429, "too_large": 413, "bad_frame": 400, "unavailable": 503}
MAX_WAIT = 30 * SECONDS  # longest long poll the server holds, whatever the client asks


def _ip(request: web.Request) -> str | None:
    return request.headers.get("X-Forwarded-For") or request.remote  # set by the reverse proxy


def _cred(request: web.Request) -> str:
    auth = request.headers.get("Authorization", "")
    if not auth.startswith("Bearer ") or len(auth) < 8:
        raise BrokerError("unauthorized", "missing bearer credential")
    return auth[7:]


def _num(request: web.Request, name: str, default: float | None) -> float | None:
    raw = request.query.get(name)
    if raw is None:
        return default
    try:
        v = float(raw)
    except ValueError:
        raise BrokerError("bad_frame", f"{name} must be a number") from None
    if v != v:  # NaN
        raise BrokerError("bad_frame", f"{name} must be a number")
    return v


async def _json(request: web.Request):
    try:
        return await request.json()
    except ValueError:
        raise BrokerError("bad_frame", "body must be JSON") from None


@web.middleware
async def errors(request: web.Request, handler) -> web.StreamResponse:
    try:
        return await handler(request)
    except BrokerError as e:
        if e.code in ("unauthorized", "forbidden"):
            log.warning("%s %s %s denied: %s", _ip(request), request.method, request.path, e)
        headers = {"Retry-After": "1"} if e.code == "queue_full" else {}
        return web.json_response({"error": e.code, "detail": e.detail}, status=STATUS[e.code], headers=headers)
    except web.HTTPRequestEntityTooLarge:
        return web.json_response({"error": "too_large", "detail": "body too large"}, status=413)


def make_app(core: BrokerCore) -> web.Application:
    r = web.RouteTableDef()
    sid = lambda q: q.match_info["sid"]  # noqa: E731

    @r.post("/sessions")
    async def register(q):
        s, cred = await core.register(q.headers.get("X-Master-Secret", ""))
        log.info("session %s registered from %s", s[:6], _ip(q))
        return web.json_response({"session_id": s, "credential": cred}, status=201)

    @r.post("/sessions/{sid}/nodes")
    async def mint(q):
        body = await _json(q)
        addrs = body.get("addresses") if isinstance(body, dict) else None
        if not isinstance(addrs, list) or not all(isinstance(a, str) for a in addrs):
            raise BrokerError("bad_frame", "addresses must be a list of strings")
        return web.json_response(await core.mint(sid(q), _cred(q), addrs))

    @r.delete("/sessions/{sid}")
    async def revoke(q):
        await core.revoke(sid(q), _cred(q))
        log.info("session %s revoked", sid(q)[:6])
        return web.Response(status=204)

    @r.post("/sessions/{sid}/heartbeat")
    async def heartbeat(q):
        return web.json_response({"remaining": await core.heartbeat(sid(q), _cred(q))})

    @r.get("/sessions/{sid}/alive", allow_head=False)
    async def alive(q):
        return web.json_response({"remaining": await core.alive(sid(q), _cred(q))})

    @r.get("/sessions/{sid}/presence", allow_head=False)
    async def presence(q):
        return web.json_response(await core.presence(sid(q), _cred(q)))

    @r.get("/sessions/{sid}/stats", allow_head=False)
    async def stats(q):
        return web.json_response(await core.stats(sid(q), _cred(q)))

    @r.get("/sessions/{sid}/info", allow_head=False)
    async def info(q):
        i = await core.info(sid(q), _cred(q))
        return web.json_response({"spec_version": i.spec_version, "max_frame": i.max_frame, "frame_ttl": i.frame_ttl})

    @r.post("/sessions/{sid}/send/{dest}")
    async def send(q):
        await core.send(sid(q), _cred(q), q.match_info["dest"], await q.read(), _num(q, "ttl", None))
        return web.Response(status=204)

    @r.get("/sessions/{sid}/recv", allow_head=False)
    async def recv(q):
        got = await core.recv(sid(q), _cred(q), min(max(_num(q, "wait", 0), 0), MAX_WAIT))
        if not got:
            return web.Response(status=204)
        heads = []  # per record: 4-byte length, 8-byte delivery id, 1-byte src length, src (then the frame itself)
        for d in got:
            src = d.src.encode()
            heads.append(struct.pack(">IQB", 9 + len(src) + len(d.frame), d.delivery_id, len(src)) + src)
        resp = web.StreamResponse(headers={"Content-Type": "application/octet-stream"})
        resp.content_length = sum(len(h) + len(d.frame) for h, d in zip(heads, got))
        await resp.prepare(q)
        for h, d in zip(heads, got):  # write the frames as they are: no concatenated copy
            await resp.write(h)
            await resp.write(d.frame)
        return resp

    @r.post("/sessions/{sid}/ack")
    async def ack(q):
        body = await _json(q)
        ids = body.get("ids") if isinstance(body, dict) else None
        if not isinstance(ids, list) or not all(isinstance(i, int) for i in ids):
            raise BrokerError("bad_frame", "ids must be a list of integers")
        await core.ack(sid(q), _cred(q), ids)
        return web.Response(status=204)

    async def sweeper(app):
        async def loop():
            while True:
                await asyncio.sleep(1)
                core.sweep()
        task = asyncio.ensure_future(loop())
        yield
        task.cancel()

    # a frame plus the JSON/headers around it; core.send then reports too_large with a proper code
    app = web.Application(client_max_size=core.lim.max_frame + MiB, middlewares=[errors])
    app.add_routes(r)
    app.cleanup_ctx.append(sweeper)
    return app


def limits_from_env() -> Limits:
    """LEASE_SECONDS and RUN_TTL, and any other limit as BROKER_<NAME> (for example BROKER_SENDER_SHARE=1.0)."""
    lim = Limits(lease=float(os.environ.get("LEASE_SECONDS", Limits.lease)), ttl=float(os.environ.get("RUN_TTL", Limits.ttl)))
    for f in dataclasses.fields(Limits):
        raw = os.environ.get(f"BROKER_{f.name.upper()}")
        if raw is not None:
            setattr(lim, f.name, type(getattr(lim, f.name))(raw))
    return lim


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    lim = limits_from_env()
    core = BrokerCore(os.environ["MASTER_SECRET"], lim)
    web.run_app(make_app(core), host=os.environ.get("HOST", "127.0.0.1"), port=int(os.environ.get("PORT", 8080)),
                access_log=None)  # behind a reverse proxy on the same host, so loopback by default


if __name__ == "__main__":
    main()
