"""Broker: the only listener. Every caller is identified by the first label of the Host
header: <token>.BASE_DOMAIN. A server token is created with the master secret and may
poll/reply/mint; client tokens (SuperNodes) may only POST their run's Fleet paths."""

import asyncio
import hmac
import json
import os
import re
import secrets
import time
import uuid
from dataclasses import dataclass, field

from aiohttp import web

BASE_DOMAIN = os.environ["BASE_DOMAIN"].lower()  # e.g. broker.example.com
MASTER_SECRET = os.environ["MASTER_SECRET"].encode()  # only authorises /register
RUN_TTL = int(os.environ.get("RUN_TTL", 6 * 3600))  # seconds until an unrevoked run expires
LEASE = int(os.environ.get("LEASE_SECONDS", 120))  # heartbeat lease; 0 = no lease, only RUN_TTL applies
VERBOSE = os.environ.get("VERBOSE") == "1"  # also log every request (noisy: /next, /heartbeat, /alive)
REQ_TIMEOUT = 60  # seconds a SuperNode waits for the SuperLink's reply
POLL_TIMEOUT = 25  # seconds a wrapper poll waits for work
TOKEN_RE = re.compile(r"[0-9a-f]{16,63}")  # one DNS label, lowercase hex
FLEET = {  # allowlist; keeps the unauthenticated Control/Runtime APIs off this port
    f"/v1/fleet/{p}"
    for p in (
        "register-node activate-node deactivate-node unregister-node send-node-heartbeat "
        "pull-messages push-messages get-run get-fab push-object pull-object "
        "confirm-message-received"
    ).split()
}
DROP = {"host", "content-length", "connection", "transfer-encoding", "keep-alive",
        "accept-encoding", "content-encoding"}


@dataclass
class Run:
    token: str  # the server token
    secret: str  # proves the caller on a server-token host is the real wrapper
    expires: float = field(default_factory=lambda: time.time() + RUN_TTL)
    lease: float = field(default_factory=lambda: time.time() + LEASE if LEASE else float("inf"))
    queue: asyncio.Queue = field(default_factory=asyncio.Queue)
    pending: dict[str, asyncio.Future] = field(default_factory=dict)
    clients: dict[str, bool] = field(default_factory=dict)  # client token -> has registered
    last_poll: float = 0.0
    wrapper_up: bool = False  # only to log wrapper connect/silent once per change
    stats: dict[str, list] = field(default_factory=dict)  # path -> [count, total_seconds]


runs: dict[str, Run] = {}  # server token -> run
clients: dict[str, Run] = {}  # client token -> run


def log(*a) -> None:
    print(*a, flush=True)  # never log secrets or full tokens: use S()/C()/tag()


def S(tok: str) -> str:  # server token (the run / its wrapper)
    return f"[S]{tok[:6]}"


def C(tok: str) -> str:  # client token (one SuperNode)
    return f"[C]{tok[:6]}"


def tag(tok: str | None) -> str:  # role from the tables; [?] if unknown or already dropped
    if not tok:
        return "[?]-"
    return S(tok) if tok in runs else C(tok) if tok in clients else f"[?]{tok[:6]}"


def ip(request: web.Request) -> str | None:
    # nginx sets X-Forwarded-For to the real peer; spoofable if the broker is exposed directly
    return request.headers.get("X-Forwarded-For") or request.remote


def host_token(request: web.Request) -> str | None:
    host = request.host.split(":")[0].lower()
    tok = host.removesuffix("." + BASE_DOMAIN)
    return tok if tok != host and TOKEN_RE.fullmatch(tok) else None


def drop(run: Run, why: str) -> None:
    log(f"{S(run.token)} run ended: {why}")
    runs.pop(run.token, None)
    run.queue.put_nowait(None)  # wake a /next poller blocked on this run
    for c in run.clients:
        clients.pop(c, None)
    for fut in run.pending.values():
        if not fut.done():
            fut.set_result((503, {}, b"run ended"))


def lookup(table: dict[str, Run], tok: str | None) -> Run | None:
    run = table.get(tok)
    if run and min(run.expires, run.lease) < time.time():  # lazy check; sweep() covers idle runs
        drop(run, "lease lapsed" if run.lease < run.expires else "RUN_TTL reached")
        return None
    return run


async def sweep() -> None:
    while True:
        await asyncio.sleep(1)
        for run in list(runs.values()):
            lookup(runs, run.token)


def same(a: str, b: bytes) -> bool:
    return hmac.compare_digest(a.encode(), b)


def server_run(request: web.Request) -> Run:
    run = lookup(runs, host_token(request))
    if not run or not same(request.headers.get("X-Run-Secret", ""), run.secret.encode()):
        log(f"{tag(host_token(request))} {ip(request)} {request.path!r} denied: "
            f"{'wrong run secret' if run else 'no live run'}")
        raise web.HTTPForbidden()
    return run


async def register(request: web.Request) -> web.Response:
    tok = host_token(request)
    if not tok or not same(request.headers.get("X-Master-Secret", ""), MASTER_SECRET):
        log(f"{tag(tok)} {ip(request)} register denied: wrong master secret")
        return web.Response(status=403)
    if lookup(runs, tok) or tok in clients:
        log(f"{tag(tok)} {ip(request)} register denied: token in use")
        return web.Response(status=409)
    run = runs[tok] = Run(tok, secrets.token_urlsafe(32))
    log(f"{S(tok)} {ip(request)} registered (lease={str(LEASE) + 's' if LEASE else 'off'}, ttl={RUN_TTL}s)")
    return web.json_response({"run_secret": run.secret})


async def mint(request: web.Request) -> web.Response:
    run = server_run(request)
    n = (await request.json()).get("n")
    if not isinstance(n, int) or not 1 <= n <= 64:
        return web.Response(status=400, text="n must be 1..64")
    toks = [secrets.token_hex(16) for _ in range(n)]
    for t in toks:
        run.clients[t] = False
        clients[t] = run
    log(f"{S(run.token)} minted {n} client tokens")
    return web.json_response({"tokens": toks})


async def run_status(request: web.Request) -> web.Response:
    return web.json_response(server_run(request).clients)


async def revoke(request: web.Request) -> web.Response:
    run = runs.get(host_token(request))  # no lookup: an expired run is already gone -> 204
    if run:
        if not same(request.headers.get("X-Run-Secret", ""), run.secret.encode()):
            log(f"{S(run.token)} {ip(request)} revoke denied: wrong run secret")
            raise web.HTTPForbidden()
        drop(run, "revoked")
    return web.Response(status=204)


async def heartbeat(request: web.Request) -> web.Response:
    run = server_run(request)
    if LEASE:
        run.lease = time.time() + LEASE
    return web.json_response({"remaining": round(min(run.expires, run.lease) - time.time(), 1)})


async def alive(request: web.Request) -> web.Response:
    tok = host_token(request)
    if not tok:
        return web.Response(status=404)
    run = lookup(runs, tok) or lookup(clients, tok)
    if not run:  # expired, revoked and never-existed look the same on purpose
        return web.Response(status=410)
    return web.json_response({"remaining": round(min(run.expires, run.lease) - time.time(), 1)})


async def fleet(request: web.Request) -> web.Response:
    tok = host_token(request)
    run = lookup(clients, tok)
    path = request.path
    if not run or path not in FLEET:
        return web.Response(status=404, text="not found")
    if time.time() - run.last_poll > POLL_TIMEOUT + 5:
        if run.wrapper_up:
            run.wrapper_up = False
            log(f"{S(run.token)} wrapper silent; Fleet calls get 503")
        return web.Response(status=503, text="no SuperLink wrapper connected")
    t0 = time.time()
    rid = uuid.uuid4().hex
    fut = run.pending[rid] = asyncio.get_running_loop().create_future()
    hdrs = {k: v for k, v in request.headers.items() if k.lower() not in DROP}
    await run.queue.put((rid, path, hdrs, await request.read()))
    try:
        status, rhdrs, body = await asyncio.wait_for(fut, REQ_TIMEOUT)
    except asyncio.TimeoutError:
        log(f"{S(run.token)} <- {C(tok)} {path} -> 504 wrapper did not reply in {REQ_TIMEOUT}s")
        return web.Response(status=504, text="SuperLink did not reply in time")
    finally:
        run.pending.pop(rid, None)
    dt = time.time() - t0
    s = run.stats.setdefault(path, [0, 0.0])
    s[0] += 1
    s[1] += dt
    if path.endswith("/register-node") and status < 300 and tok in run.clients:
        run.clients[tok] = True
    log(f"{S(run.token)} <- {C(tok)} {ip(request)} POST {path} -> {status} {dt*1000:.1f}ms "
          f"req={request.content_length}B resp={len(body)}B")
    return web.Response(status=status, headers=rhdrs, body=body)


async def tunnel_next(request: web.Request) -> web.Response:
    run = server_run(request)
    run.last_poll = time.time()
    if not run.wrapper_up:
        run.wrapper_up = True
        log(f"{S(run.token)} {ip(request)} wrapper connected")
    deadline = time.time() + POLL_TIMEOUT
    try:
        while True:
            item = await asyncio.wait_for(run.queue.get(), max(deadline - time.time(), 0))
            if item is None:  # run ended while polling
                return web.Response(status=403)
            rid, path, hdrs, body = item
            if rid in run.pending:  # else the client already got a 504; don't replay it
                break
    except asyncio.TimeoutError:
        if request.transport:  # a hung-up poll is not proof the wrapper is alive
            run.last_poll = time.time()
        return web.Response(status=204)
    if request.transport is None:  # wrapper hung up while we waited: hand the request to a live poll
        run.queue.put_nowait(item)
        return web.Response(status=204)
    run.last_poll = time.time()
    return web.Response(body=body, headers={
        "X-Req-Id": rid, "X-Req-Path": path, "X-Req-Headers": json.dumps(hdrs)})


async def tunnel_reply(request: web.Request) -> web.Response:
    run = server_run(request)
    fut = run.pending.get(request.match_info["rid"])
    if fut and not fut.done():
        fut.set_result((int(request.headers["X-Status"]),
                        json.loads(request.headers["X-Resp-Headers"]), await request.read()))
    return web.Response(status=204)


async def stats_view(request: web.Request) -> web.Response:
    return web.json_response({p: {"n": n, "avg_ms": round(t / n * 1000, 2)}
                              for p, (n, t) in server_run(request).stats.items()})


@web.middleware
async def access(request: web.Request, handler) -> web.StreamResponse:
    t0, status, who = time.time(), 500, tag(host_token(request))  # before the handler can drop the run
    try:
        resp = await handler(request)
        status = resp.status
        return resp
    except web.HTTPException as e:
        status = e.status
        raise
    finally:
        if who.startswith("[?]"):  # e.g. /register creates the run inside the handler
            who = tag(host_token(request))
        gone = " (client hung up)" if request.transport is None else ""  # aiohttp keeps running the handler
        log(f"{who} {ip(request)} {request.method} {request.path!r} "
            f"-> {status} {(time.time() - t0)*1000:.1f}ms{gone}")


async def main() -> None:
    app = web.Application(client_max_size=1 << 30, middlewares=[access] if VERBOSE else [])
    app.router.add_post("/register", register)
    app.router.add_post("/mint", mint)
    app.router.add_get("/status", run_status)
    app.router.add_post("/revoke", revoke)
    app.router.add_post("/heartbeat", heartbeat)
    app.router.add_get("/alive", alive)
    app.router.add_get("/next", tunnel_next)
    app.router.add_post("/reply/{rid}", tunnel_reply)
    app.router.add_get("/stats", stats_view)
    app.router.add_post("/{tail:.*}", fleet)  # last: everything else is a client Fleet call
    r = web.AppRunner(app, access_log=None)
    await r.setup()
    # behind nginx on the same host, so loopback by default
    await web.TCPSite(r, os.environ.get("HOST", "127.0.0.1"), int(os.environ.get("PORT", 8080))).start()
    print(f"broker: *.{BASE_DOMAIN} on :{os.environ.get('PORT', 8080)}", flush=True)
    await sweep()


asyncio.run(main())
