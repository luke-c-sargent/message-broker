"""The store behind a broker: queues with a visibility timeout. It knows nothing about credentials, limits,
lanes or waiting: the broker core does all of that. To the store a queue is just a name. Entries expire on their
own (`expires` is on the same clock the store was given), so a store may also drop them with its own timer."""

import time
from collections import deque
from dataclasses import dataclass, field
from typing import Callable, Protocol


@dataclass
class Entry:
    id: int  # assigned by the broker core; stable across redeliveries
    src: str
    frame: bytes
    expires: float


class Store(Protocol):
    async def create(self, sid: str) -> None:
        """A session exists. Called once, before any other call for `sid`."""

    async def push(self, sid: str, queue: str, entry: Entry) -> None:
        """Queue an entry."""

    async def pop(self, sid: str, queue: str, max_bytes: int, visible_for: float) -> list[tuple[int, Entry]]:
        """Return at once, never waiting: `(delivery_id, entry)` pairs, up to `max_bytes` of frames but always
        at least one if any is queued, or [] if none is. An entry that is not acked within `visible_for`
        seconds is queued again and gets a new delivery id. An expired entry is never returned."""

    async def ack(self, sid: str, queue: str, delivery_ids: list[int]) -> list[int]:
        """Remove delivered entries. Returns the entry ids actually removed (unknown ids are ignored)."""

    async def purge(self, sid: str) -> None:
        """The session ended: delete everything."""


@dataclass
class _Queue:
    queue: deque = field(default_factory=deque)  # of Entry
    inflight: dict = field(default_factory=dict)  # delivery_id -> (Entry, ack deadline)


class MemoryStore:
    """The default store: one process, nothing survives a restart."""

    def __init__(self, clock: Callable[[], float] = time.monotonic):
        self.now, self._q, self._ids = clock, {}, 0  # _q: sid -> queue name -> _Queue

    async def create(self, sid: str) -> None:
        self._q[sid] = {}

    async def push(self, sid: str, queue: str, entry: Entry) -> None:
        qs = self._q.get(sid)
        if qs is not None:
            qs.setdefault(queue, _Queue()).queue.append(entry)

    def _tidy(self, q: _Queue) -> None:
        now = self.now()
        for did, (e, deadline) in sorted(q.inflight.items(), reverse=True):  # newest first: appendleft keeps order
            if deadline <= now:
                del q.inflight[did]
                q.queue.appendleft(e)
        for e in [e for e in q.queue if e.expires <= now]:
            q.queue.remove(e)

    async def pop(self, sid: str, queue: str, max_bytes: int, visible_for: float) -> list[tuple[int, Entry]]:
        q = self._q.get(sid, {}).get(queue)
        if q is None:
            return []
        self._tidy(q)
        out, size = [], 0
        while q.queue and (not out or size + len(q.queue[0].frame) <= max_bytes):
            e = q.queue.popleft()
            self._ids += 1
            q.inflight[self._ids] = (e, self.now() + visible_for)
            out.append((self._ids, e))
            size += len(e.frame)
        return out

    async def ack(self, sid: str, queue: str, delivery_ids: list[int]) -> list[int]:
        q = self._q.get(sid, {}).get(queue)
        done = []
        for did in delivery_ids:
            if q and did in q.inflight:
                done.append(q.inflight.pop(did)[0].id)
        return done

    async def purge(self, sid: str) -> None:
        self._q.pop(sid, None)
