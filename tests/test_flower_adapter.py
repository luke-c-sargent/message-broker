"""The Flower adapter against a fake SuperLink and a real broker. No flwr needed."""

import asyncio
from contextlib import asynccontextmanager

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from message_broker import BrokerCore, Endpoint, Launcher, Limits, connect
from message_broker.adapters.flower import run_hub, spoke_app
from message_broker.broker.server import make_app


class FakeSuperLink:
    def __init__(self):
        self.hits = []  # (path, headers, body)
        app = web.Application()
        app.router.add_get("/", self.root)
        app.router.add_post("/{tail:.*}", self.handle)
        self.server = TestServer(app)

    async def root(self, request):
        return web.Response(text="ok")

    async def handle(self, request):
        body = await request.read()
        self.hits.append((request.path, dict(request.headers), body))
        return web.Response(status=201, body=body[::-1], headers={"X-Reply": "yes", "Content-Type": "application/protobuf"})


@asynccontextmanager
async def stack():
    lim = Limits(max_frame=64 * 1024, frame_ttl=10, ack_timeout=0.5, lease=0)
    broker = TestServer(make_app(BrokerCore("m", lim)))
    await broker.start_server()
    fake = FakeSuperLink()
    await fake.server.start_server()
    http = aiohttp.ClientSession()
    async with Launcher(str(broker.make_url("")), "m", ["hub", "c1"]) as launch:
        hub = asyncio.ensure_future(run_hub(connect(launch.connection_string("hub")),
                                            f"http://127.0.0.1:{fake.server.port}", limit=4, poll_wait=0.3))
        conn = connect(launch.connection_string("c1"))
        ep = Endpoint(conn, poll_wait=0.3, pollers=2)
        await ep.start()
        spoke = TestServer(spoke_app(ep))
        await spoke.start_server()
        try:
            yield launch, fake, hub, http, f"http://127.0.0.1:{spoke.port}"
        finally:
            await http.close()
            await spoke.close()
            await ep.close()
            await conn.close()
            hub.cancel()
            await asyncio.gather(hub, return_exceptions=True)
            await fake.server.close()
    await broker.close()


async def test_request_is_replayed_and_the_answer_comes_back():
    async with stack() as (_, fake, _, http, spoke):
        async with http.post(f"{spoke}/v1/fleet/get-run", data=b"hello", headers={"X-Custom": "1"}) as r:
            assert (r.status, await r.read()) == (201, b"olleh")
            assert r.headers["X-Reply"] == "yes" and r.headers["Content-Type"] == "application/protobuf"
        path, headers, body = fake.hits[0]
        assert (path, body, headers["X-Custom"]) == ("/v1/fleet/get-run", b"hello", "1")


async def test_a_large_body_crosses_many_frames_intact():
    big = bytes(range(256)) * 2000  # 500 KB over 64 KiB frames, both ways
    async with stack() as (_, _, _, http, spoke):
        async with http.post(f"{spoke}/v1/fleet/push-object", data=big) as r:
            assert (r.status, await r.read()) == (201, big[::-1])


async def test_paths_outside_the_allowlist_never_reach_the_superlink():
    async with stack() as (_, fake, _, http, spoke):
        for path in ("/v1/control/start-run", "/v1/fleet/../control/x", "/"):
            async with http.post(spoke + path, data=b"x") as r:
                assert r.status == 404, path
        assert fake.hits == []


async def test_only_post_is_forwarded():
    async with stack() as (_, fake, _, http, spoke):
        async with http.get(f"{spoke}/v1/fleet/get-run") as r:
            assert r.status == 405
        assert fake.hits == []


async def test_a_dead_superlink_becomes_502():
    async with stack() as (_, fake, _, http, spoke):
        await fake.server.close()
        async with http.post(f"{spoke}/v1/fleet/get-run", data=b"x") as r:
            assert r.status == 502


async def test_the_end_of_the_session_becomes_403_and_stops_the_hub():
    async with stack() as (launch, _, hub, http, spoke):
        await launch.wait_joined(timeout=5)
        await launch.client.revoke(launch.sid, launch.admin)
        await asyncio.sleep(0.5)
        async with http.post(f"{spoke}/v1/fleet/get-run", data=b"x") as r:
            assert r.status == 403  # a SuperNode stops cleanly on 403 (spec 11.1)
        assert (await asyncio.wait_for(hub, 3)).code == "session_gone"


async def test_a_session_that_ended_before_joining_is_reported_not_raised():
    broker = TestServer(make_app(BrokerCore("m")))
    await broker.start_server()
    async with Launcher(str(broker.make_url("")), "m", ["hub"]) as launch:
        conn = connect(launch.connection_string("hub"))
        await launch.client.revoke(launch.sid, launch.admin)
        fake = FakeSuperLink()
        await fake.server.start_server()
        err = await asyncio.wait_for(run_hub(conn, f"http://127.0.0.1:{fake.server.port}"), 5)
        assert err.code == "session_gone"
        await conn.close()
        await fake.server.close()
    await broker.close()
