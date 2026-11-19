"""Store contract (the interface in broker/store.py). Every store runs the same tests: add one to STORES."""

import time

import pytest

from message_broker import MemoryStore
from message_broker.broker import Entry

STORES = {"memory": MemoryStore}


class Clock:
    def __init__(self):
        self.skew = 0.0

    def __call__(self) -> float:
        return time.monotonic() + self.skew


@pytest.fixture
def clock():
    return Clock()


@pytest.fixture(params=STORES)
async def store(request, clock):
    s = STORES[request.param](clock)
    await s.create("s")
    return s


def entry(i, frame=b"x", ttl=100, clock=None):
    return Entry(i, "a", frame, (clock() if clock else time.monotonic()) + ttl)


async def test_expired_entries_leave_a_queue_nobody_pops(store, clock):
    await store.push("s", "b", entry(1, ttl=1, clock=clock))
    await store.pop("s", "b", 100, 5)  # delivered, never acked
    await store.push("s", "b", entry(2, ttl=1, clock=clock))
    clock.skew += 10
    await store.push("s", "b", entry(3, ttl=100, clock=clock))
    assert store.holds("s", "b") == 1  # the push dropped 1 and 2; no pop was needed
    assert [e.id for _, e in await store.pop("s", "b", 100, 5)] == [3]


async def test_push_pop_ack(store):
    await store.push("s", "b", entry(1, b"one"))
    await store.push("s", "b", entry(2, b"two"))
    got = await store.pop("s", "b", 100, 5)
    assert [e.frame for _, e in got] == [b"one", b"two"]
    assert await store.ack("s", "b", [d for d, _ in got] + [999]) == [1, 2]  # unknown ids are ignored
    assert await store.ack("s", "b", [got[0][0]]) == []  # a repeated ack removes nothing
    assert await store.pop("s", "b", 100, 5) == []


async def test_pop_respects_max_bytes_but_always_returns_one(store):
    for i in (1, 2, 3):
        await store.push("s", "b", entry(i, b"x" * 10))
    assert len(await store.pop("s", "b", 25, 5)) == 2
    assert len(await store.pop("s", "b", 1, 5)) == 1  # one frame, though it is over the byte limit


async def test_unacked_entry_comes_back_with_a_new_delivery_id(store, clock):
    await store.push("s", "b", entry(1, clock=clock))
    [(d1, e1)] = await store.pop("s", "b", 100, 5)
    assert await store.pop("s", "b", 100, 5) == []
    clock.skew += 6
    [(d2, e2)] = await store.pop("s", "b", 100, 5)
    assert e2.id == e1.id and d2 != d1
    assert await store.ack("s", "b", [d1]) == []  # the old delivery is no longer valid
    assert await store.ack("s", "b", [d2]) == [1]


async def test_expired_entries_are_never_returned(store, clock):
    await store.push("s", "b", entry(1, ttl=1, clock=clock))
    clock.skew += 2
    assert await store.pop("s", "b", 100, 5) == []


async def test_pop_never_waits(store):
    t = time.monotonic()
    assert await store.pop("s", "b", 100, 5) == []
    assert time.monotonic() - t < 0.5


async def test_queues_are_independent(store):
    await store.push("s", "b", entry(1, b"small"))
    await store.push("s", "b#bulk", entry(2, b"large" * 10))
    assert [e.id for _, e in await store.pop("s", "b#bulk", 1000, 5)] == [2]
    assert [e.id for _, e in await store.pop("s", "b", 1000, 5)] == [1]


async def test_purge_deletes_everything(store):
    await store.push("s", "b", entry(1))
    await store.purge("s")
    assert await store.pop("s", "b", 100, 5) == []
    await store.push("s", "b", entry(2))  # a push to a gone session is ignored, not an error
    assert await store.pop("s", "b", 100, 5) == []
