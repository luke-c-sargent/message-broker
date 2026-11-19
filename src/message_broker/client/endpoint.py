"""Endpoint (spec sections 5-7): a node's end of a conversation, as RPC channels over a Connection.
Chunking, ack on receipt, duplicate removal, in-order reassembly and closed-channel memory.
Stream mode is not implemented yet."""

import asyncio
import itertools
import logging
import secrets
import time
from dataclasses import dataclass, field
from typing import Awaitable, Callable

from ..errors import RETRYABLE, BrokerError
from ..frames import Header, decode, encode
from ..types import SECONDS, MiB
from .connection import Connection

log = logging.getLogger("message_broker.endpoint")
POLLERS = 4  # concurrent recv calls (docs/measurements.md: few, so a slow link is not asked for more than ack_timeout allows)
POLL_WAIT = 25 * SECONDS  # one long poll
RPC_DEADLINE = 25 * SECONDS  # under Flower's 30 s call timeout (spec 11.1)
PRUNE_EVERY = 1 * SECONDS  # closed-channel memory is cleaned at most this often, not on every frame
HEADER_SLACK = 64  # bytes: a later frame's header may be longer (a larger seq, `more`)
TRANSPORT = (OSError, asyncio.TimeoutError)  # network trouble: same as `unavailable`


@dataclass
class Reply:
    payload: bytes | bytearray | memoryview = b""  # any bytes-like object
    status: str = "ok"
    meta: dict = field(default_factory=dict)


Handler = Callable[[str, bytes, dict], Awaitable[Reply]]  # (src, request payload, request meta)


class _Rx:
    """Reassembles one message (one direction of one channel) from frames that may arrive in any order."""

    def __init__(self, now: float):
        self.expected, self.buf, self.buffered = 0, {}, 0  # buf: frames that arrived ahead of their turn
        self.data, self.meta, self.status, self.mode, self.seen = bytearray(), {}, "ok", None, now

    def feed(self, h: Header, payload: bytes, now: float, limit: int) -> str:
        """-> "dup" | "pending" | "done" | "overflow" """
        self.seen = now
        if h.seq < self.expected or h.seq in self.buf:
            return "dup"
        self.buf[h.seq] = (h, payload)
        self.buffered += len(payload)
        while self.expected in self.buf:
            fh, fp = self.buf.pop(self.expected)
            self.buffered -= len(fp)
            if fh.seq == 0:
                self.meta, self.mode = fh.meta, fh.mode
            if fh.status:
                self.status = fh.status
            self.data += fp  # the message grows in place; the frame can be freed at once
            self.expected += 1
            if not fh.more:
                return "done"
        return "overflow" if self.buffered > limit else "pending"


@dataclass
class _Call:
    rx: _Rx
    fut: asyncio.Future


class Endpoint:
    def __init__(self, conn: Connection, handler: Handler | None = None, *, pollers: int = POLLERS,
                 poll_wait: float = POLL_WAIT, rpc_deadline: float = RPC_DEADLINE, reorder_bytes: int = 64 * MiB,
                 backoff: tuple[float, float] = (0.05 * SECONDS, 0.25 * SECONDS), clock: Callable[[], float] = time.monotonic):
        self.conn, self.handler, self.pollers, self.poll_wait = conn, handler, pollers, poll_wait
        self.rpc_deadline, self.reorder_bytes, self.backoff, self.now = rpc_deadline, reorder_bytes, backoff, clock
        self._prefix, self._n = secrets.token_hex(4), itertools.count()  # channel ids are never reused
        self._calls: dict[tuple[str, str], _Call] = {}
        self._inbound: dict[tuple[str, str], _Rx] = {}
        self._tomb: dict[tuple[str, str], float] = {}  # closed inbound channels -> forget-after time (rising: oldest first)
        self._pruned = float("-inf")
        self._tasks: set[asyncio.Task] = set()
        self._error: BrokerError | None = None
        self._closed = asyncio.Event()
        self.max_frame = self.frame_ttl = 0

    async def start(self) -> None:
        info = await self.conn.info()
        self.max_frame, self.frame_ttl = info.max_frame, info.frame_ttl
        self.rpc_deadline = min(self.rpc_deadline, self.frame_ttl)  # a request must not outlive frame_ttl
        for _ in range(self.pollers):
            self._spawn(self._poll())

    async def close(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        self._fail(BrokerError("session_gone", "session closed"))

    def _spawn(self, coro) -> None:
        t = asyncio.ensure_future(coro)
        self._tasks.add(t)
        t.add_done_callback(self._tasks.discard)

    async def wait_closed(self) -> BrokerError:
        """Wait until the endpoint cannot continue (the session ended, or the credential stopped working)."""
        await self._closed.wait()
        return self._error

    def _fail(self, e: BrokerError) -> None:
        self._error = self._error or e
        self._closed.set()
        for c in self._calls.values():
            if not c.fut.done():
                c.fut.set_exception(self._error)

    # ---- sending -------------------------------------------------------------------------
    async def _send_frame(self, dest: str, frame: bytes, ttl: float, until: float) -> None:
        delay = self.backoff[0]
        while True:
            try:
                return await self.conn.send(dest, frame, ttl)  # a repeat of the same frame is safe (6.3)
            except BrokerError as e:
                if e.code not in RETRYABLE:
                    raise
            except TRANSPORT:
                pass
            if self.now() + delay >= until:
                raise BrokerError("timeout", "could not send before the deadline")
            await asyncio.sleep(delay)
            delay = min(delay * 2, self.backoff[1])

    async def _send_message(self, dest: str, ch: str, payload: bytes, *, rsp: bool, status: str | None,
                            meta: dict, ttl: float, until: float) -> None:
        first = Header(ch, 0, "close" if rsp else "open", None if rsp else "rpc", False, rsp, status, meta)
        room = self.max_frame - 4 - len(encode(first)) - HEADER_SLACK
        if room <= 0:
            raise ValueError("meta is too large for max_frame")
        view, count = memoryview(payload), max(1, -(-len(payload) // room))  # chunks are views: no copy of the message
        for i in range(count):
            h = Header(ch, i, "close" if rsp else ("open" if i == 0 else "data"), None if rsp or i else "rpc",
                       i < count - 1, rsp, status, meta if i == 0 else {})
            await self._send_frame(dest, encode(h, view[i * room:(i + 1) * room]),
                                   max(min(ttl, until - self.now()), 0.001), until)

    async def call(self, dest: str, payload: bytes, meta: dict | None = None,
                   deadline: float | None = None) -> Reply:
        """One RPC. Raises BrokerError("timeout") when the deadline passes; the caller decides on a retry."""
        if self._error:
            raise self._error
        deadline = min(deadline or self.rpc_deadline, self.frame_ttl)
        ch = f"{self._prefix}-{next(self._n)}"
        call = self._calls[(dest, ch)] = _Call(_Rx(self.now()), asyncio.get_running_loop().create_future())
        until = self.now() + deadline

        async def run() -> Reply:
            await self._send_message(dest, ch, payload, rsp=False, status=None, meta=meta or {},
                                     ttl=deadline, until=until)
            return await call.fut
        try:
            return await asyncio.wait_for(run(), deadline)
        except asyncio.TimeoutError:
            raise BrokerError("timeout", f"no reply from {dest} in {deadline}s") from None
        finally:
            self._calls.pop((dest, ch), None)  # a late reply for this channel is now dropped

    # ---- receiving -----------------------------------------------------------------------
    async def _poll(self) -> None:
        delay = self.backoff[0]
        while True:
            try:
                got = await self.conn.recv(self.poll_wait)
                if got:
                    try:  # ack on receipt, before handling (7.1); a lost ack only causes a duplicate
                        await self.conn.ack([d.delivery_id for d in got])
                    except BrokerError as e:
                        if e.code not in RETRYABLE:
                            raise
                    except TRANSPORT:
                        pass
                    for d in got:
                        self._on_frame(d.src, d.frame)
                delay = self.backoff[0]
            except BrokerError as e:
                if e.code not in RETRYABLE:
                    self._fail(e)
                    return
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.backoff[1])
            except TRANSPORT:
                await asyncio.sleep(delay)
                delay = min(delay * 2, self.backoff[1])

    def _prune(self) -> None:
        now = self.now()
        if now < self._pruned + PRUNE_EVERY:
            return
        self._pruned = now
        while self._tomb:  # forget times only rise, so the expired ones are all at the front
            k = next(iter(self._tomb))
            if self._tomb[k] > now:
                break
            del self._tomb[k]
        for k in [k for k, rx in self._inbound.items() if rx.seen + 2 * self.frame_ttl <= now]:
            del self._inbound[k]  # a message that never completed (the opener gave up)

    def _on_frame(self, src: str, frame: bytes) -> None:
        try:
            h, payload = decode(frame)
        except BrokerError as e:
            log.warning("dropping a bad frame from %s: %s", src, e)
            return
        self._prune()
        now = self.now()
        if h.rsp:  # a reply to a call we opened
            call = self._calls.get((src, h.ch))
            if not call or call.fut.done():
                return
            state = call.rx.feed(h, payload, now, self.reorder_bytes)
            if state == "done":
                call.fut.set_result(Reply(call.rx.data, call.rx.status, call.rx.meta))
            elif state == "overflow":
                call.fut.set_result(Reply(b"", "bad_frame"))
            return
        key = (src, h.ch)
        if key in self._tomb:
            return  # a late copy of a channel we already finished: never run it twice
        rx = self._inbound.setdefault(key, _Rx(now))
        state = rx.feed(h, payload, now, self.reorder_bytes)
        if state in ("done", "overflow"):
            del self._inbound[key]
            self._tomb[key] = now + 2 * self.frame_ttl
            if state == "overflow":
                self._spawn(self._respond(src, h.ch, Reply(b"", "bad_frame")))
            elif rx.mode == "rpc" and self.handler:
                self._spawn(self._serve(src, h.ch, rx.data, rx.meta))
            else:
                log.warning("dropping a request from %s: no handler or unsupported mode %r", src, rx.mode)

    async def _serve(self, src: str, ch: str, payload: bytearray, meta: dict) -> None:
        try:
            reply = await self.handler(src, payload, meta)
        except Exception:
            log.exception("handler failed")
            reply = Reply(b"", "unavailable")
        await self._respond(src, ch, reply)

    async def _respond(self, src: str, ch: str, reply: Reply) -> None:
        try:
            await self._send_message(src, ch, reply.payload, rsp=True, status=reply.status, meta=reply.meta,
                                     ttl=self.frame_ttl, until=self.now() + self.frame_ttl)
        except BrokerError as e:
            log.warning("could not send the reply to %s: %s", src, e)
