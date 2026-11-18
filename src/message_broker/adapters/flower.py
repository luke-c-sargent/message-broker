"""Flower adapter (spec section 11.1). It speaks Flower's HTTP only and does not import flwr.

  spoke: beside a SuperNode. A local HTTP server the SuperNode connects to. Each request becomes an RPC to the hub.
  hub:   beside the SuperLink. Replays allowed Fleet calls to the local SuperLink and sends the answer back.

Run one with: python -m message_broker.adapters.flower hub|spoke   (settings in environment variables, see main)"""

import asyncio
import json
import os
import sys

import aiohttp
from aiohttp import web

from ..client import Endpoint, Reply, connect
from ..errors import BrokerError
from ..types import SECONDS, MiB

HUB = "hub"  # the address the orchestrator gives the SuperLink-side node (spec section 11)
FLEET = {  # allowlist, enforced on the hub side: keeps the unauthenticated Control/Runtime APIs off the broker
    f"/v1/fleet/{p}"
    for p in (
        "register-node activate-node deactivate-node unregister-node send-node-heartbeat "
        "pull-messages push-messages get-run get-fab push-object pull-object "
        "confirm-message-received"
    ).split()
}
REQ_DROP = {"host", "content-length", "connection", "transfer-encoding", "keep-alive",
            "accept-encoding", "content-encoding"}
RESP_DROP = {"content-length", "connection", "transfer-encoding", "keep-alive", "content-encoding",
             "date", "server"}
# What the SuperNode accepts (spec 11.1): 503/504 are retried forever, 403 stops it cleanly, others make it give up.
STATUS = {"session_gone": 403, "unauthorized": 403, "forbidden": 403, "timeout": 504, "queue_full": 503,
          "unavailable": 503, "unknown_dest": 503, "peer_gone": 503, "too_large": 502, "bad_frame": 502,
          "conflict": 502}


def spoke_app(endpoint: Endpoint, hub: str = HUB, deadline: float = 25 * SECONDS) -> web.Application:
    """The local server a SuperNode talks to. The deadline stays under Flower's 30 s call timeout."""
    async def forward(request: web.Request) -> web.Response:
        if request.method != "POST":
            return web.Response(status=405)
        headers = {k: v for k, v in request.headers.items() if k.lower() not in REQ_DROP}
        try:
            r = await endpoint.call(hub, await request.read(),
                                    {"path": request.path, "headers": json.dumps(headers)}, deadline)
        except BrokerError as e:
            return web.Response(status=STATUS[e.code], text=f"broker: {e}")
        if r.status != "ok":
            return web.Response(status=STATUS.get(r.status, 502), text=f"hub: {r.status}")
        return web.Response(status=int(r.meta.get("status", 502)), body=r.payload,
                            headers=json.loads(r.meta.get("headers", "{}")))

    app = web.Application(client_max_size=1024 * MiB)  # model updates can be large
    app.router.add_route("*", "/{tail:.*}", forward)
    return app


def hub_handler(local: str, http: aiohttp.ClientSession, limit: int = 16, timeout: float = 20 * SECONDS):
    """Handler for the hub's Endpoint. `limit` bounds the calls in flight to the local SuperLink."""
    sem = asyncio.Semaphore(limit)

    async def handle(src: str, payload: bytes, meta: dict) -> Reply:
        path = meta.get("path", "")
        if path not in FLEET:
            return Reply(b"not found", "ok", {"status": "404", "headers": "{}"})
        async with sem:
            try:
                async with http.post(local + path, headers=json.loads(meta.get("headers", "{}")), data=payload,
                                     timeout=aiohttp.ClientTimeout(total=timeout)) as u:
                    body = await u.read()
                    rh = {k: v for k, v in u.headers.items() if k.lower() not in RESP_DROP}
                    return Reply(body, "ok", {"status": str(u.status), "headers": json.dumps(rh)})
            except (aiohttp.ClientError, asyncio.TimeoutError, ValueError) as e:  # the SuperLink is down or slow
                return Reply(repr(e).encode(), "ok", {"status": "502", "headers": "{}"})
    return handle


async def run_hub(conn, local: str = "http://127.0.0.1:8000", limit: int = 16, poll_wait: float = 25 * SECONDS) -> BrokerError:
    """Serve until the session ends. Returns the reason."""
    async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None)) as http:
        while True:  # do not join the session until the SuperLink is up
            try:
                async with http.get(local + "/"):
                    break
            except aiohttp.ClientError:
                await asyncio.sleep(0.5)
        endpoint = Endpoint(conn, hub_handler(local, http, limit), pollers=limit, poll_wait=poll_wait)
        try:
            await endpoint.start()
            return await endpoint.wait_closed()
        except BrokerError as e:  # the session ended or the credential is wrong before we could join
            return e
        finally:
            await endpoint.close()


async def run_spoke(conn, port: int, host: str = "127.0.0.1", poll_wait: float = 25 * SECONDS) -> BrokerError:
    """Serve a SuperNode until the session ends. Returns the reason."""
    endpoint = Endpoint(conn, pollers=4, poll_wait=poll_wait)
    runner = web.AppRunner(spoke_app(endpoint), access_log=None)
    await runner.setup()
    try:
        await endpoint.start()
        await web.TCPSite(runner, host, port).start()
        return await endpoint.wait_closed()
    except BrokerError as e:
        return e
    finally:
        await runner.cleanup()
        await endpoint.close()


def main() -> None:
    """hub: BROKER_URL, SUPERLINK (default http://127.0.0.1:8000), POLLERS (default 16).
    spoke: BROKER_URL, LISTEN_PORT, LISTEN_HOST (default 127.0.0.1)."""
    role = sys.argv[1] if len(sys.argv) > 1 else ""
    if role == "hub":
        coro = run_hub(connect(), os.environ.get("SUPERLINK", "http://127.0.0.1:8000"),
                       int(os.environ.get("POLLERS", 16)))
    elif role == "spoke":
        coro = run_spoke(connect(), int(os.environ["LISTEN_PORT"]), os.environ.get("LISTEN_HOST", "127.0.0.1"))
    else:
        sys.exit("usage: python -m message_broker.adapters.flower hub|spoke")
    err = asyncio.run(coro)
    print(f"flower {role} adapter stopped: {err}", flush=True)
    sys.exit(0 if err.code == "session_gone" else 1)  # session_gone: the session ended normally


if __name__ == "__main__":
    main()
