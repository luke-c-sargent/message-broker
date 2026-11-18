"""Broker core: sessions, credentials, address rules, lease and TTL, sender stamping, limits, lanes, waiting,
presence. It holds all the policy. Entries live in a Store, which only queues; the core keeps a small ledger of
every entry (lane, sender, size, expiry) so quotas and expiry are exact whatever store sits behind it.

Lanes: each address has two queues in the store, `small` (frames up to `small_frame_bytes`, such as heartbeats)
and `bulk` (the rest). Each lane has its own limits, and `recv` serves both, so a small frame never waits
behind a queue full of large ones."""

import asyncio
import hashlib
import hmac
import heapq
import re
import secrets
import time
from dataclasses import dataclass, field
from typing import Callable

from ..errors import BrokerError
from ..types import HOURS, KiB, MINUTES, SECONDS, SPEC_VERSION, Delivery, Info, MiB
from .store import Entry, MemoryStore, Store

ADDRESS = re.compile(r"[A-Za-z0-9_-]{1,64}")
SMALL, BULK = 0, 1  # lanes. The store queue of a lane: the address, or the address plus "#bulk" (no address has a #)


@dataclass
class Limits:  # every default is provisional (spec section 12)
    max_frame: int = 4 * MiB
    small_frame_bytes: int = 64 * KiB  # frames up to this size use the small lane; 0 turns the small lane off
    queue_frames: int = 1024  # per lane
    queue_bytes: int = 64 * MiB  # bulk lane, per address
    small_queue_bytes: int = 4 * MiB  # small lane, per address
    session_bytes: int = 256 * MiB  # bulk lane, all addresses together
    sender_share: float = 0.25  # of each lane's limits
    max_nodes: int = 64
    frame_ttl: float = 2 * MINUTES
    ack_timeout: float = 5 * SECONDS
    recv_recheck: float = 0.5 * SECONDS  # a waiting recv looks again this often (a store redelivers silently)
    lease: float = 2 * MINUTES  # 0 turns the lease off
    ttl: float = 6 * HOURS
    recv_batch_bytes: int = 8 * MiB


def _h(cred: str) -> str:  # store hashes so a lookup does not leak the credential by timing
    return hashlib.sha256(cred.encode()).hexdigest()


@dataclass
class _Lane:
    frames: int = 0  # queued + delivered-but-unacked
    bytes: int = 0
    by_sender: dict = field(default_factory=dict)  # src -> [frames, bytes]


@dataclass
class _Node:
    address: str
    cred: str  # hash
    joined: bool = False
    last_seen: float | None = None
    lanes: tuple = field(default_factory=lambda: (_Lane(), _Lane()))
    wake: asyncio.Event = field(default_factory=asyncio.Event)  # set when something may be ready for recv


@dataclass
class _Session:
    id: str
    admin: str  # hash
    lease_deadline: float
    ttl_deadline: float
    nodes: dict = field(default_factory=dict)  # address -> _Node
    by_cred: dict = field(default_factory=dict)  # hash -> _Node
    bytes: int = 0  # bulk lane, all addresses
    ledger: dict = field(default_factory=dict)  # entry id -> (node, lane, src, size)
    expiry: list = field(default_factory=list)  # heap of (expires, entry id)


class BrokerCore:
    def __init__(self, master_secret: str, limits: Limits | None = None, store: Store | None = None,
                 clock: Callable[[], float] = time.monotonic):
        self.master, self.lim, self.now = master_secret.encode(), limits or Limits(), clock
        self.store = store or MemoryStore(clock)
        self.sessions: dict[str, _Session] = {}
        self._entry_ids, self._tasks = 0, set()

    # ---- helpers -------------------------------------------------------------------------
    def _live(self, sid: str) -> _Session | None:
        s = self.sessions.get(sid)
        if s and (self.now() >= s.ttl_deadline or self.now() >= s.lease_deadline):
            self._drop(s)
            return None
        return s

    def _drop(self, s: _Session) -> None:
        self.sessions.pop(s.id, None)
        for n in s.nodes.values():
            n.wake.set()  # waiting recv calls look again and find the session gone
        t = asyncio.ensure_future(self.store.purge(s.id))
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    def _auth(self, sid: str, cred: str, admin: bool | None = None) -> tuple[_Session, _Node | None]:
        s = self._live(sid)
        if not s:
            raise BrokerError("session_gone")
        h = _h(cred)
        if hmac.compare_digest(h, s.admin):
            node = None
        elif h in s.by_cred:
            node = s.by_cred[h]
        else:
            raise BrokerError("unauthorized")
        if admin is True and node is not None or admin is False and node is None:
            raise BrokerError("forbidden")
        return s, node

    @staticmethod
    def _queue(address: str, lane: int) -> str:
        return address + "#bulk" if lane == BULK else address

    def _release(self, s: _Session, entry_id: int) -> None:
        meta = s.ledger.pop(entry_id, None)  # None: already released (acked or expired)
        if meta:
            node, lane, src, size = meta
            c = node.lanes[lane]
            c.frames -= 1
            c.bytes -= size
            c.by_sender[src][0] -= 1
            c.by_sender[src][1] -= size
            if lane == BULK:
                s.bytes -= size

    def _expire(self, s: _Session) -> None:
        """Release entries whose ttl ended, so limits count only frames that can still be delivered."""
        while s.expiry and s.expiry[0][0] <= self.now():
            self._release(s, heapq.heappop(s.expiry)[1])

    # ---- control plane -------------------------------------------------------------------
    async def register(self, master_secret: str) -> tuple[str, str]:
        if not hmac.compare_digest(master_secret.encode(), self.master):
            raise BrokerError("unauthorized")
        now, sid, cred = self.now(), secrets.token_hex(16), secrets.token_urlsafe(32)
        self.sessions[sid] = _Session(sid, _h(cred), now + (self.lim.lease or float("inf")), now + self.lim.ttl)
        await self.store.create(sid)
        return sid, cred

    async def mint(self, sid: str, cred: str, addresses: list[str]) -> dict[str, str]:
        s, _ = self._auth(sid, cred, admin=True)
        if len(set(addresses)) != len(addresses) or not all(ADDRESS.fullmatch(a) for a in addresses):
            raise BrokerError("bad_frame", "addresses must be unique, 1-64 chars of A-Za-z0-9_-")
        if any(a in s.nodes and s.nodes[a].joined for a in addresses):
            raise BrokerError("conflict", "address has already joined")
        if len(set(s.nodes) | set(addresses)) > self.lim.max_nodes:
            raise BrokerError("bad_frame", f"at most {self.lim.max_nodes} nodes")
        out = {}
        for a in addresses:  # all checks passed: a repeat for an unjoined address rotates its credential
            if a in s.nodes:
                s.by_cred.pop(s.nodes[a].cred, None)
            out[a] = secrets.token_urlsafe(32)
            n = s.nodes.get(a) or s.nodes.setdefault(a, _Node(a, ""))
            n.cred = _h(out[a])
            s.by_cred[n.cred] = n
        return out

    async def revoke(self, sid: str, cred: str) -> None:
        if self._live(sid):  # idempotent: a gone session is already revoked
            s, _ = self._auth(sid, cred, admin=True)
            self._drop(s)

    async def heartbeat(self, sid: str, cred: str) -> float:
        s, _ = self._auth(sid, cred, admin=True)
        if self.lim.lease:
            s.lease_deadline = self.now() + self.lim.lease
        return min(s.ttl_deadline, s.lease_deadline) - self.now()

    async def alive(self, sid: str, cred: str) -> float:
        s, _ = self._auth(sid, cred)
        return min(s.ttl_deadline, s.lease_deadline) - self.now()

    async def presence(self, sid: str, cred: str) -> dict:
        s, _ = self._auth(sid, cred, admin=True)
        return {a: {"joined": n.joined, "last_seen": n.last_seen} for a, n in s.nodes.items()}

    async def stats(self, sid: str, cred: str) -> dict:
        s, _ = self._auth(sid, cred, admin=True)
        self._expire(s)
        return {"session_bytes": s.bytes,
                "nodes": {a: {"frames": sum(c.frames for c in n.lanes), "bytes": sum(c.bytes for c in n.lanes)}
                          for a, n in s.nodes.items()}}

    def sweep(self) -> None:
        """Drop sessions whose lease or ttl ended. The server calls this every second, so a launcher
        that died without revoking does not leak its session."""
        for sid in list(self.sessions):
            self._live(sid)

    # ---- data plane ----------------------------------------------------------------------
    def connection(self, sid: str, cred: str):
        from ..client.connection import BoundConnection  # the core does not otherwise depend on the client
        return BoundConnection(self, sid, cred)

    async def info(self, sid: str, cred: str) -> Info:
        self._auth(sid, cred)
        return Info(SPEC_VERSION, self.lim.max_frame, self.lim.frame_ttl)

    async def send(self, sid: str, cred: str, dest: str, frame: bytes, ttl: float | None) -> None:
        s, me = self._auth(sid, cred, admin=False)
        to, lim, now = s.nodes.get(dest), self.lim, self.now()
        if not to:
            raise BrokerError("unknown_dest", dest)
        if len(frame) > lim.max_frame:
            raise BrokerError("too_large", f"{len(frame)} > {lim.max_frame}")
        if ttl is not None and ttl <= 0:
            raise BrokerError("bad_frame", "ttl must be positive")
        self._expire(s)
        lane = SMALL if len(frame) <= lim.small_frame_bytes else BULK
        c = to.lanes[lane]
        cap_bytes, floor = ((lim.small_queue_bytes, lim.small_frame_bytes) if lane == SMALL
                            else (lim.queue_bytes, lim.max_frame))
        ps = c.by_sender.setdefault(me.address, [0, 0])
        if (c.frames + 1 > lim.queue_frames or c.bytes + len(frame) > cap_bytes
                or (lane == BULK and s.bytes + len(frame) > lim.session_bytes)
                or ps[0] + 1 > max(1, int(lim.queue_frames * lim.sender_share))
                or ps[1] + len(frame) > max(floor, int(cap_bytes * lim.sender_share))):
            raise BrokerError("queue_full", dest)
        self._entry_ids += 1
        eid, expires = self._entry_ids, now + min(ttl or lim.frame_ttl, lim.frame_ttl)
        s.ledger[eid] = (to, lane, me.address, len(frame))  # count first: the push below awaits
        heapq.heappush(s.expiry, (expires, eid))
        c.frames += 1
        c.bytes += len(frame)
        ps[0] += 1
        ps[1] += len(frame)
        if lane == BULK:
            s.bytes += len(frame)
        try:
            await self.store.push(sid, self._queue(dest, lane), Entry(eid, me.address, frame, expires))
        except BaseException:
            self._release(s, eid)
            raise
        me.joined, me.last_seen = True, now
        to.wake.set()

    async def _take(self, s: _Session, me: _Node) -> list[Delivery]:
        """Without waiting: up to `recv_batch_bytes`, half from the small lane first. A lane that has less than its
        half gives the rest to the other, and the bulk lane always gets room, so neither lane starves the other."""
        budget, hold = self.lim.recv_batch_bytes, self.lim.ack_timeout
        got = []  # (lane, delivery id, entry)

        async def pop(lane: int, room: int) -> int:
            taken = await self.store.pop(s.id, self._queue(me.address, lane), room, hold)
            got.extend((lane, did, e) for did, e in taken)
            return sum(len(e.frame) for _, e in taken)

        used = await pop(SMALL, budget // 2)
        used += await pop(BULK, budget - used)
        if used < budget and any(lane == SMALL for lane, _, _ in got):
            await pop(SMALL, budget - used)  # the bulk lane was short: the small lane may have more to give
        return [Delivery(did << 1 | lane, e.src, e.frame) for lane, did, e in got]  # the lane rides in the low bit

    async def recv(self, sid: str, cred: str, wait: float) -> list[Delivery]:
        s, me = self._auth(sid, cred, admin=False)
        me.joined, me.last_seen = True, self.now()
        end = self.now() + wait
        while True:
            s, me = self._auth(sid, cred, admin=False)  # raises session_gone once the session ends
            self._expire(s)
            me.wake.clear()  # before looking: a send after this sets it again
            got = await self._take(s, me)
            if got:
                me.last_seen = self.now()
                return got
            left = end - self.now()
            if left <= 0:
                return []
            try:  # woken by a send or the end of the session; otherwise look again: the store may have redelivered
                await asyncio.wait_for(me.wake.wait(), min(left, self.lim.recv_recheck))
            except asyncio.TimeoutError:
                pass

    async def ack(self, sid: str, cred: str, ids: list[int]) -> None:
        s, me = self._auth(sid, cred, admin=False)
        me.last_seen = self.now()
        for lane in (SMALL, BULK):
            mine = [i >> 1 for i in ids if i & 1 == lane]
            if mine:
                for eid in await self.store.ack(sid, self._queue(me.address, lane), mine):
                    self._release(s, eid)
