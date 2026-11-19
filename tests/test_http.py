"""HTTP mailbox beyond the conformance suite: the session layer over real HTTP, the connection string,
server-side expiry, and agreement between docs/openapi.yaml and the real routes."""

import asyncio
from pathlib import Path

import pytest
import yaml
from aiohttp import web
from aiohttp.test_utils import TestServer
from openapi_spec_validator import validate

from message_broker import Limits, BrokerError, BrokerCore, Reply, Endpoint
from message_broker.client.http import BrokerClient, connect
from message_broker.broker.server import make_app

SPEC = Path(__file__).parent.parent / "docs" / "openapi.yaml"


async def test_rpc_over_http_with_connection_strings():
    mb = BrokerCore("m", Limits(max_frame=400, frame_ttl=5, ack_timeout=0.5, lease=0))
    server = TestServer(make_app(mb))
    await server.start_server()
    host = f"127.0.0.1:{server.port}"
    admin = BrokerClient(f"http://{host}")
    sid, acred = await admin.register("m")
    creds = await admin.mint(sid, acred, ["hub", "c1"])

    async def echo(src, payload, meta):
        return Reply(payload[::-1], "ok", {"from": src})
    s_node, c_node = connect(f"http://{creds['hub']}@{host}/{sid}"), connect(f"http://{creds['c1']}@{host}/{sid}")
    srv, cli = Endpoint(s_node, echo, poll_wait=0.5), Endpoint(c_node, poll_wait=0.5)
    await srv.start()
    await cli.start()
    try:
        data = bytes(range(256)) * 20  # 5 KiB: many frames of at most 400 bytes
        r = await cli.call("hub", data)
        assert (r.payload, r.status, r.meta) == (data[::-1], "ok", {"from": "c1"})
    finally:
        await cli.close()
        await srv.close()
        await s_node.close()
        await c_node.close()
        await admin.close()
        await server.close()


async def test_connect_needs_credential_and_session():
    with pytest.raises(ValueError):
        connect("http://host:1/sid")
    with pytest.raises(ValueError):
        connect("http://cred@host:1/")


async def test_server_drops_expired_sessions_without_any_request():
    mb = BrokerCore("m", Limits(lease=0.05))
    server = TestServer(make_app(mb))
    await server.start_server()
    http = BrokerClient(str(server.make_url("")))
    try:
        sid, admin = await http.register("m")
        await asyncio.sleep(1.5)  # the sweeper runs every second
        assert sid not in mb.sessions
    finally:
        await http.close()
        await server.close()


async def test_garbage_requests_get_error_codes_not_500s():
    server = TestServer(make_app(BrokerCore("m")))
    await server.start_server()
    http = BrokerClient(str(server.make_url("")))
    try:
        sid, admin = await http.register("m")
        for path, kw in [(f"/sessions/{sid}/nodes", dict(data=b"not json")),
                         (f"/sessions/{sid}/nodes", dict(json={"addresses": "a"})),
                         (f"/sessions/{sid}/ack", dict(json={"ids": ["x"]}))]:
            with pytest.raises(BrokerError) as e:
                await http._call("POST", path, admin, **kw)
            assert e.value.code == "bad_frame", path
        for q in ("?wait=abc", "?wait=nan"):
            with pytest.raises(BrokerError) as e:
                await http._call("GET", f"/sessions/{sid}/recv{q}", admin)
            assert e.value.code in ("bad_frame", "forbidden")  # the admin is not a node: either is a clean refusal
        with pytest.raises(BrokerError) as e:
            await http._call("GET", f"/sessions/{sid}/alive")  # no credential
        assert e.value.code == "unauthorized"
    finally:
        await http.close()
        await server.close()


async def test_a_reply_without_a_broker_body_is_a_clean_error_not_a_retry():
    server = TestServer(web.Application())  # not a broker: every path is a plain 404
    await server.start_server()
    http = BrokerClient(str(server.make_url("")))
    try:
        with pytest.raises(BrokerError) as e:
            await http.info("s", "c")
        assert e.value.code == "unknown_dest"  # not "unavailable", which an endpoint retries forever
    finally:
        await http.close()
        await server.close()


def test_openapi_file_is_valid_and_matches_the_routes():
    spec = yaml.safe_load(SPEC.read_text())
    validate(spec)
    documented = {(m.upper(), p) for p, item in spec["paths"].items() for m in item if m != "parameters"}
    app = make_app(BrokerCore("m"))
    real = {(r.method, r.resource.canonical) for r in app.router.routes()}
    assert documented == real
