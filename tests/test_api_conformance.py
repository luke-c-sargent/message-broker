"""Mailbox conformance (spec section 13). Every backend runs the same tests: add one to BACKENDS.
A backend object needs: register, mint, revoke, renew, alive, presence, stats, node(sid, cred)."""

import asyncio
import time

import pytest

from aiohttp.test_utils import TestServer

from message_broker import Limits, BrokerError, BrokerCore
from message_broker.client import BoundConnection
from message_broker.client.http import BrokerClient
from message_broker.broker.server import make_app

SECRET = "master"


class Clock:
    """Real time plus a skew the test can jump, so waiting recv calls still end."""

    def __init__(self):
        self.skew = 0.0

    def __call__(self) -> float:
        return time.monotonic() + self.skew


async def memory(limits, clock, cleanup):
    return BrokerCore(SECRET, limits, clock=clock)


async def http(limits, clock, cleanup):
    """The in-memory mailbox behind the real HTTP server, reached through the real HTTP client."""
    server = TestServer(make_app(BrokerCore(SECRET, limits, clock=clock)))
    await server.start_server()
    client = BrokerClient(str(server.make_url("")))
    cleanup += [client.close, server.close]
    return client


BACKENDS = {"memory": memory, "http": http}


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture(params=BACKENDS)
async def make(request, clock):
    cleanup = []

    async def factory(**kw):
        return await BACKENDS[request.param](Limits(**kw), clock, cleanup)
    yield factory
    for close in cleanup:
        await close()


async def session(mb, *addrs):
    sid, admin = await mb.register(SECRET)
    creds = await mb.mint(sid, admin, list(addrs))
    return sid, admin, {a: BoundConnection(mb, sid, c) for a, c in creds.items()}


def frame(n: int = 1) -> bytes:
    return b"x" * n


async def code(coro) -> str:
    with pytest.raises(BrokerError) as e:
        await coro
    return e.value.code


async def test_register_needs_master_secret(make):
    assert await code((await make()).register("wrong")) == "unauthorized"


async def test_send_recv_stamps_src(make):
    sid, _, n = await session(await make(), "a", "b")
    await n["a"].send("b", b"hello")
    [d] = await n["b"].recv(0)
    assert (d.src, d.frame) == ("a", b"hello")  # src comes from the credential, not the frame
    await n["b"].ack([d.delivery_id])


async def test_node_cannot_use_admin_operations(make):
    mb = await make()
    sid, admin, n = await session(mb, "a", "b")
    node_cred = n["a"].cred
    assert await code(mb.mint(sid, node_cred, ["c"])) == "forbidden"
    assert await code(mb.renew(sid, node_cred)) == "forbidden"
    assert await code(mb.presence(sid, node_cred)) == "forbidden"
    assert await code(mb.send(sid, admin, "a", b"x", None)) == "forbidden"  # the admin is not a node
    assert await code(mb.recv(sid, admin, 0)) == "forbidden"
    assert await code(mb.renew(sid, "nonsense")) == "unauthorized"
    assert await code(mb.renew("no-such-session", admin)) == "session_gone"


async def test_nodes_and_sessions_are_isolated(make):
    mb = await make()
    sid1, _, n1 = await session(mb, "a", "b", "c")
    sid2, _, n2 = await session(mb, "a", "z")
    await n1["a"].send("b", b"for b")
    assert await n1["c"].recv(0) == []  # c cannot see b's mail
    assert await code(n1["a"].send("z", b"x")) == "unknown_dest"  # z exists only in session 2
    assert await n2["z"].recv(0) == []
    assert await code(mb.alive(sid2, n1["a"].cred)) == "unauthorized"  # a credential works in its own session only


async def test_unknown_dest_and_too_large(make):
    sid, _, n = await session(await make(max_frame=100), "a", "b")
    assert await code(n["a"].send("nobody", b"x")) == "unknown_dest"
    assert await code(n["a"].send("b", frame(101))) == "too_large"
    await n["a"].send("b", frame(100))


async def test_queue_and_session_limits(make):
    sid, _, n = await session(await make(queue_frames=2, sender_share=1.0), "a", "b")
    await n["a"].send("b", b"1")
    await n["a"].send("b", b"2")
    assert await code(n["a"].send("b", b"3")) == "queue_full"
    sid, _, n = await session(await make(session_bytes=10, max_frame=10, queue_bytes=10, sender_share=1.0,
                                         small_frame_bytes=0), "a", "b")  # every frame is bulk
    await n["a"].send("b", frame(6))
    assert await code(n["a"].send("b", frame(6))) == "queue_full"


async def test_sender_share_keeps_room_for_others(make):
    sid, _, n = await session(await make(queue_frames=8, sender_share=0.25), "a", "b", "srv")
    await n["a"].send("srv", b"1")
    await n["a"].send("srv", b"2")
    assert await code(n["a"].send("srv", b"3")) == "queue_full"  # a used its share of 2
    await n["b"].send("srv", b"hb")  # b is not starved


async def test_ack_and_redelivery(make, clock):
    sid, _, n = await session(await make(ack_timeout=5), "a", "b")
    await n["a"].send("b", b"m")
    [d1] = await n["b"].recv(0)
    assert await n["b"].recv(0) == []  # delivered, waiting for the ack
    clock.skew += 6
    [d2] = await n["b"].recv(0)  # no ack in time: delivered again
    assert d2.frame == b"m" and d2.delivery_id != d1.delivery_id
    await n["b"].ack([d1.delivery_id, d2.delivery_id])  # an ack of either delivery is fine; unknown ids are ignored
    clock.skew += 6
    assert await n["b"].recv(0) == []


async def test_ttl_drops_stale_frames_and_frees_room(make, clock):
    mb = await make()
    sid, admin, n = await session(mb, "a", "b")
    await n["a"].send("b", b"old", ttl=1)
    clock.skew += 2
    assert await n["b"].recv(0) == []
    assert (await mb.stats(sid, admin))["nodes"]["b"]["frames"] == 0
    assert await code(n["a"].send("b", b"x", ttl=0)) == "bad_frame"


async def test_late_ack_of_an_expired_frame_does_not_count_twice(make, clock):
    mb = await make()
    sid, admin, n = await session(mb, "a", "b")
    await n["a"].send("b", b"m", ttl=1)
    [d] = await n["b"].recv(0)
    clock.skew += 2  # the ttl ends while the frame is delivered but not acked
    await n["b"].ack([d.delivery_id])
    stats = await mb.stats(sid, admin)
    assert stats["session_bytes"] == 0 and stats["nodes"]["b"] == {"frames": 0, "bytes": 0}


async def test_competing_consumers_get_each_frame_once(make):
    sid, _, n = await session(await make(), "a", "b")
    waiting = [asyncio.ensure_future(n["b"].recv(0.2)) for _ in range(2)]
    await asyncio.sleep(0.02)
    await n["a"].send("b", b"one")
    got = sorted((len(r) for r in await asyncio.gather(*waiting)))
    assert got == [0, 1]


async def test_mint_rules(make):
    mb = await make(max_nodes=3)
    sid, admin, n = await session(mb, "a", "b")
    assert await code(mb.mint(sid, admin, ["bad address"])) == "bad_frame"
    assert await code(mb.mint(sid, admin, ["c", "c"])) == "bad_frame"
    assert await code(mb.mint(sid, admin, ["c", "d"])) == "bad_frame"  # 4 nodes > max_nodes
    old = n["a"].cred
    new = (await mb.mint(sid, admin, ["a"]))["a"]  # a has not joined: a repeat rotates the credential
    assert new != old
    assert await code(BoundConnection(mb, sid, old).recv(0)) == "unauthorized"
    await BoundConnection(mb, sid, new).recv(0)  # joins
    assert await code(mb.mint(sid, admin, ["a"])) == "conflict"


async def test_presence_and_alive(make, clock):
    mb = await make()
    sid, admin, n = await session(mb, "a", "b")
    assert (await mb.presence(sid, admin))["a"] == {"joined": False, "idle": None}
    await n["a"].send("b", b"x")
    p = await mb.presence(sid, admin)
    assert p["a"]["joined"] and not p["b"]["joined"]
    assert await mb.alive(sid, n["b"].cred) > 0
    clock.skew += 5
    assert 5 <= (await mb.presence(sid, admin))["a"]["idle"] < 6  # seconds since last seen, not a raw clock


async def test_lease_renewal_ttl_and_revoke(make, clock):
    mb = await make(lease=10, session_ttl=100)
    sid, admin, n = await session(mb, "a", "b")
    clock.skew += 8
    await mb.renew(sid, admin)
    clock.skew += 8
    await mb.alive(sid, admin)  # renewed
    clock.skew += 3
    assert await code(mb.alive(sid, admin)) == "session_gone"  # lease lapsed
    mb = await make(lease=0, session_ttl=100)
    sid, admin, n = await session(mb, "a", "b")
    clock.skew += 99
    await mb.alive(sid, admin)
    clock.skew += 2
    assert await code(mb.alive(sid, admin)) == "session_gone"  # hard ttl, renewals do not extend it
    mb = await make()
    sid, admin, n = await session(mb, "a", "b")
    pending = asyncio.ensure_future(n["b"].recv(5))
    await asyncio.sleep(0.02)
    await mb.revoke(sid, admin)
    with pytest.raises(BrokerError) as e:
        await pending
    assert e.value.code == "session_gone"
    await mb.revoke(sid, admin)  # idempotent


# ---- lanes (spec 7.4): small frames never wait behind bulk ones -----------------------------------------------

LANES = dict(small_frame_bytes=10, max_frame=100, sender_share=1.0)  # 10 bytes or fewer: small; more: bulk


async def test_a_small_frame_is_delivered_before_bulk_frames_queued_earlier(make):
    sid, _, n = await session(await make(**LANES), "a", "b")
    for _ in range(3):
        await n["a"].send("b", frame(50))
    await n["a"].send("b", b"beat")
    got = await n["b"].recv(0)
    assert got[0].frame == b"beat" and len(got) == 4  # the small frame comes first; the bulk ones follow


async def test_a_full_bulk_lane_does_not_stop_small_frames(make):
    sid, _, n = await session(await make(queue_bytes=100, **LANES), "a", "b")
    await n["a"].send("b", frame(50))
    await n["a"].send("b", frame(50))
    assert await code(n["a"].send("b", frame(50))) == "queue_full"  # bulk lane full
    await n["a"].send("b", b"beat")  # but a heartbeat still gets in


async def test_the_small_lane_has_its_own_limit(make):
    sid, _, n = await session(await make(small_queue_bytes=20, **LANES), "a", "b")
    await n["a"].send("b", frame(10))
    await n["a"].send("b", frame(10))
    assert await code(n["a"].send("b", frame(1))) == "queue_full"
    await n["a"].send("b", frame(50))  # bulk is unaffected


async def test_neither_lane_starves_the_other(make):
    """A flood of small frames cannot keep a bulk frame waiting, and the reverse (half of each batch each)."""
    sid, _, n = await session(await make(recv_batch_bytes=100, **LANES), "a", "b")
    for _ in range(40):
        await n["a"].send("b", frame(10))
    await n["a"].send("b", frame(40))  # one bulk frame behind 40 small ones
    first = await n["b"].recv(0)
    assert sum(len(d.frame) for d in first) >= 40 and any(len(d.frame) == 40 for d in first)  # bulk got in at once
    assert sum(1 for d in first if len(d.frame) == 10) <= 6  # and the small lane did not take everything
    sid, _, n = await session(await make(recv_batch_bytes=100, **LANES), "a", "b")
    for _ in range(3):
        await n["a"].send("b", frame(40))
    await n["a"].send("b", b"beat")
    first = await n["b"].recv(0)
    assert first[0].frame == b"beat" and sum(len(d.frame) for d in first) < 190  # bulk is capped by the batch too


async def test_acks_reach_the_right_lane(make, clock):
    mb = await make(ack_timeout=5, **LANES)
    sid, admin, n = await session(mb, "a", "b")
    await n["a"].send("b", b"beat")
    await n["a"].send("b", frame(50))
    small, bulk = await n["b"].recv(0)
    await n["b"].ack([bulk.delivery_id])  # only the bulk frame is acked
    clock.skew += 6
    again = await n["b"].recv(0)
    assert [d.frame for d in again] == [b"beat"]  # the small one comes back, the bulk one does not
    await n["b"].ack([d.delivery_id for d in again])
    assert (await mb.stats(sid, admin))["nodes"]["b"] == {"frames": 0, "bytes": 0}


async def test_a_waiting_recv_sees_a_redelivery_within_the_recheck_time(make):
    mb = await make(ack_timeout=0.2, recv_recheck=0.1)
    sid, _, n = await session(mb, "a", "b")
    await n["a"].send("b", b"m")
    await n["b"].recv(0)  # delivered, never acked
    t = time.monotonic()
    got = await n["b"].recv(3)  # waits; the store redelivers silently after 0.2 s
    assert got and got[0].frame == b"m" and time.monotonic() - t < 0.2 + 0.1 + 0.3
