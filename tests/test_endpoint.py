"""Endpoint layer tests, including the faults of spec sections 6.3 and 7.1: duplicates, lost replies, lost acks."""

import asyncio
from contextlib import asynccontextmanager

import pytest
from hypothesis import HealthCheck, given, settings
from hypothesis import strategies as st

from message_broker import Limits, BrokerError, BrokerCore, Reply, Endpoint
from message_broker.client import BoundConnection

FAST = dict(backoff=(0.01, 0.05), poll_wait=0.2)


class Faulty:
    """Wraps a Node. send_plan / recv_plan cycle: send: 0 ok, 1 send twice, 2 send then lose the reply;
    recv: 0 ok, 1 take the frames then lose the response (they stay unacked)."""

    def __init__(self, node, send_plan=(0,), recv_plan=(0,)):
        self.conn, self.sp, self.rp, self.sent = node, list(send_plan), list(recv_plan), []
        self.ns = self.nr = 0

    async def info(self):
        return await self.conn.info()

    async def send(self, dest, frame, ttl=None):
        self.sent.append((dest, frame))
        mode = self.sp[self.ns % len(self.sp)]
        self.ns += 1
        await self.conn.send(dest, frame, ttl)
        if mode == 1:
            await self.conn.send(dest, frame, ttl)
        if mode == 2:
            raise OSError("reply lost")

    async def recv(self, wait):
        mode = self.rp[self.nr % len(self.rp)]
        self.nr += 1
        got = await self.conn.recv(wait)
        if mode == 1 and got:
            raise OSError("response lost")
        return got

    async def ack(self, ids):
        await self.conn.ack(ids)


@asynccontextmanager
async def pair(limits=None, wrap=lambda n: n, handler=None, start_server=True, wrap_client=None, **kw):
    mb = BrokerCore("m", limits or Limits(frame_ttl=5, ack_timeout=0.05, recv_recheck=0.02, lease=0))
    sid, admin = await mb.register("m")
    creds = await mb.mint(sid, admin, ["hub", "c1"])
    calls = []

    async def echo(src, payload, meta):
        calls.append((src, payload))
        return Reply(payload, "ok", {"seen": meta.get("k", "")})

    server = Endpoint(wrap(BoundConnection(mb, sid, creds["hub"])), handler or echo, **FAST, **kw)
    client = Endpoint((wrap_client or wrap)(BoundConnection(mb, sid, creds["c1"])), **FAST, **kw)
    await client.start()
    if start_server:
        await server.start()
    try:
        yield mb, sid, admin, server, client, calls
    finally:
        await client.close()
        await server.close()


def test_closed_channel_memory_is_pruned_from_the_front_and_not_on_every_frame():
    now = [0.0]
    ep = Endpoint(None, clock=lambda: now[0])
    ep.frame_ttl = 5
    for i in range(3):
        now[0] = float(i)
        ep._tomb[("a", f"c{i}")] = now[0] + 2 * ep.frame_ttl  # forgotten at 10, 11, 12
    now[0] = 10.5
    ep._prune()
    assert list(ep._tomb) == [("a", "c1"), ("a", "c2")]
    now[0] = 11.2  # c1 is due, but the last prune was under PRUNE_EVERY ago
    ep._prune()
    assert len(ep._tomb) == 2
    now[0] = 12.0
    ep._prune()
    assert not ep._tomb


async def test_rpc_roundtrip_with_meta():
    async with pair() as (_, _, _, _, client, calls):
        r = await client.call("hub", b"ping", {"k": "v"})
        assert (r.payload, r.status, r.meta) == (b"ping", "ok", {"seen": "v"})
        assert calls == [("c1", b"ping")]


async def test_large_payload_is_chunked_and_reassembled():
    big = bytes(range(256)) * 40  # 10 KiB over 300-byte frames
    async with pair(Limits(max_frame=300, frame_ttl=5, ack_timeout=0.05, recv_recheck=0.02, lease=0)) as (_, _, _, _, client, calls):
        r = await client.call("hub", big, {"k": "v"})
        assert r.payload == big and r.meta == {"seen": "v"}
        assert calls == [("c1", big)]


async def test_handler_error_becomes_a_status():
    async def boom(src, payload, meta):
        raise RuntimeError("x")
    async with pair(handler=boom) as (_, _, _, _, client, _):
        assert (await client.call("hub", b"1")).status == "unavailable"


async def test_call_times_out():
    async def slow(src, payload, meta):
        await asyncio.sleep(5)
    async with pair(handler=slow) as (_, _, _, _, client, _):
        with pytest.raises(BrokerError) as e:
            await client.call("hub", b"1", deadline=0.2)
        assert e.value.code == "timeout"


async def test_late_copy_of_a_finished_request_does_not_run_again():
    spy = {}
    async with pair(wrap_client=lambda n: spy.setdefault("c1", Faulty(n))) as (_, _, _, _, client, calls):
        await client.call("hub", b"once")
        dest, open_frame = spy["c1"].sent[0]
        await spy["c1"].conn.send(dest, open_frame)  # the repeat of an already-accepted send arrives late
        await asyncio.sleep(0.3)
        assert len(calls) == 1


async def test_lost_recv_response_is_redelivered_and_runs_once():
    async with pair(wrap=lambda n: Faulty(n, recv_plan=(1, 0))) as (_, _, _, _, client, calls):
        r = await client.call("hub", b"x" * 700, deadline=4)
        assert r.payload == b"x" * 700 and len(calls) == 1


async def test_stale_request_is_never_delivered():
    async with pair(Limits(frame_ttl=0.2, ack_timeout=0.05, recv_recheck=0.02, lease=0), start_server=False) as (
            _, _, _, server, client, calls):
        with pytest.raises(BrokerError):
            await client.call("hub", b"old", deadline=0.1)  # nobody answers; the frame's ttl is 0.1 s
        await asyncio.sleep(0.2)
        await server.start()
        await asyncio.sleep(0.3)
        assert calls == []


async def test_backpressure_waits_instead_of_failing():
    lim = Limits(max_frame=300, queue_frames=1, sender_share=1.0, frame_ttl=5, ack_timeout=0.05, recv_recheck=0.02, lease=0)
    async with pair(lim, start_server=False) as (_, _, _, server, client, calls):
        task = asyncio.ensure_future(client.call("hub", b"y" * 700, deadline=4))  # 3 frames, queue holds 1
        await asyncio.sleep(0.3)
        assert not task.done()
        await server.start()
        assert (await task).payload == b"y" * 700 and len(calls) == 1


async def test_revoked_session_fails_pending_calls():
    async def slow(src, payload, meta):
        await asyncio.sleep(5)
    async with pair(handler=slow) as (mb, sid, admin, _, client, _):
        task = asyncio.ensure_future(client.call("hub", b"1"))
        await asyncio.sleep(0.1)
        await mb.revoke(sid, admin)
        with pytest.raises(BrokerError) as e:
            await task
        assert e.value.code == "session_gone"


@settings(max_examples=25, deadline=None, suppress_health_check=list(HealthCheck))
@given(send_plan=st.lists(st.sampled_from([0, 1, 2]), max_size=5).map(lambda p: p + [0]),  # a plan with no
       recv_plan=st.lists(st.sampled_from([0, 1]), max_size=5).map(lambda p: p + [0]),  # working step cannot finish
       sizes=st.lists(st.integers(0, 900), min_size=1, max_size=4))
def test_faults_never_cause_a_double_run_or_a_wrong_reply(send_plan, recv_plan, sizes):
    """Duplicated sends, lost send replies and lost recv responses, over chunked concurrent calls."""
    async def scenario():
        lim = Limits(max_frame=300, frame_ttl=8, ack_timeout=0.05, recv_recheck=0.02, lease=0)
        async with pair(lim, wrap=lambda n: Faulty(n, send_plan, recv_plan)) as (_, _, _, _, client, calls):
            payloads = [bytes([i]) * n for i, n in enumerate(sizes)]
            replies = await asyncio.gather(*(client.call("hub", p, deadline=8) for p in payloads))
            assert [r.payload for r in replies] == payloads  # every call got its own reply
            assert sorted(p for _, p in calls) == sorted(payloads)  # each request ran exactly once
    asyncio.run(scenario())


async def test_a_large_message_needs_little_more_memory_than_its_own_size():
    """Chunks are views and the message is rebuilt in place, so a 40 MiB message costs about 40 MiB extra at the
    peak, not 3 to 4 times that (a copy per chunk, the parts, and a join)."""
    import tracemalloc
    size = 40 * 1024 * 1024
    payload = bytes(size)

    async def sink(src, data, meta):
        return Reply(b"ok")
    async with pair(handler=sink) as (_, _, _, _, client, _):
        tracemalloc.start()
        base = tracemalloc.get_traced_memory()[0]
        tracemalloc.reset_peak()
        try:
            r = await client.call("hub", payload, deadline=4)
            extra = tracemalloc.get_traced_memory()[1] - base
        finally:
            tracemalloc.stop()
    assert r.status == "ok"
    assert extra < 1.6 * size, f"peak extra memory was {extra / size:.1f}x the message"
