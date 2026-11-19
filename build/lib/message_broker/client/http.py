"""HTTP client for a broker: the same operations as BrokerCore, plus connect()."""

import asyncio
import json
import os
import struct
from urllib.parse import unquote, urlsplit

import aiohttp

from ..errors import CODES, BrokerError
from ..types import SECONDS, Delivery, Info
from .connection import BoundConnection

REPLY_GRACE = 30 * SECONDS  # a call may take this long beyond its long-poll `wait`
FALLBACK = {401: "unauthorized", 403: "forbidden", 410: "session_gone", 404: "unknown_dest", 409: "conflict",
            429: "queue_full", 413: "too_large", 400: "bad_frame"}  # a reply with no JSON body: a proxy or the wrong server


class BrokerClient:
    def __init__(self, base_url: str):
        self.base, self._http = base_url.rstrip("/"), None

    async def close(self) -> None:
        if self._http:
            await self._http.close()

    async def _call(self, method: str, path: str, cred: str | None = None, *, wait: float = 0, **kw):
        if self._http is None:
            self._http = aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(total=None))
        headers = {"Authorization": f"Bearer {cred}"} if cred else {}
        headers.update(kw.pop("headers", {}))
        try:
            async with self._http.request(method, self.base + path, headers=headers, **kw,
                                          timeout=aiohttp.ClientTimeout(total=wait + REPLY_GRACE)) as r:
                body = await r.read()
                if r.status >= 400:
                    try:
                        code = json.loads(body)["error"]
                    except (ValueError, KeyError, TypeError):
                        code = FALLBACK.get(r.status, "unavailable")
                    raise BrokerError(code if code in CODES else "unavailable", body[:200].decode("utf-8", "replace"))
                return r.status, body
        except (aiohttp.ClientError, asyncio.TimeoutError, OSError) as e:  # the request may or may not have arrived
            raise BrokerError("unavailable", repr(e)) from e

    async def _json(self, *a, **kw):
        status, body = await self._call(*a, **kw)
        return json.loads(body) if body else None

    # ---- control plane -------------------------------------------------------------------
    async def register(self, master_secret: str) -> tuple[str, str]:
        d = await self._json("POST", "/sessions", headers={"X-Master-Secret": master_secret})
        return d["session_id"], d["credential"]

    async def mint(self, sid: str, cred: str, addresses: list[str]) -> dict[str, str]:
        return await self._json("POST", f"/sessions/{sid}/nodes", cred, json={"addresses": addresses})

    async def revoke(self, sid: str, cred: str) -> None:
        await self._call("DELETE", f"/sessions/{sid}", cred)

    async def renew(self, sid: str, cred: str) -> float:
        return (await self._json("POST", f"/sessions/{sid}/renew", cred))["remaining"]

    async def alive(self, sid: str, cred: str) -> float:
        return (await self._json("GET", f"/sessions/{sid}/alive", cred))["remaining"]

    async def presence(self, sid: str, cred: str) -> dict:
        return await self._json("GET", f"/sessions/{sid}/presence", cred)

    async def stats(self, sid: str, cred: str) -> dict:
        return await self._json("GET", f"/sessions/{sid}/stats", cred)

    # ---- data plane ----------------------------------------------------------------------
    async def info(self, sid: str, cred: str) -> Info:
        d = await self._json("GET", f"/sessions/{sid}/info", cred)
        return Info(d["spec_version"], d["max_frame"], d["frame_ttl"])

    async def send(self, sid: str, cred: str, dest: str, frame: bytes, ttl: float | None) -> None:
        await self._call("POST", f"/sessions/{sid}/send/{dest}", cred, data=frame,
                         params={} if ttl is None else {"ttl": repr(ttl)})

    async def recv(self, sid: str, cred: str, wait: float) -> list[Delivery]:
        status, body = await self._call("GET", f"/sessions/{sid}/recv", cred, wait=wait, params={"wait": repr(wait)})
        out, i, view = [], 0, memoryview(body)  # frames are views into the body: no copy
        while i < len(body):  # records: 4-byte length, 8-byte delivery id, 1-byte src length, src, frame
            (n,) = struct.unpack_from(">I", view, i)
            did, sl = struct.unpack_from(">QB", view, i + 4)
            start = i + 4 + 9
            out.append(Delivery(did, bytes(view[start:start + sl]).decode(), view[start + sl:i + 4 + n]))
            i += 4 + n
        return out

    async def ack(self, sid: str, cred: str, ids: list[int]) -> None:
        await self._call("POST", f"/sessions/{sid}/ack", cred, json={"ids": ids})


def connect(url: str | None = None) -> BoundConnection:
    """Node for a connection string `https://<credential>@host[:port]/<session_id>` (default: $BROKER_URL).
    Close it with `await node.close()`."""
    u = urlsplit(url or os.environ["BROKER_URL"])
    if not u.username or not u.path.strip("/"):
        raise ValueError("connection string needs a credential and a session id")
    host = u.hostname + (f":{u.port}" if u.port else "")
    mb = BrokerClient(f"{u.scheme}://{host}")
    return BoundConnection(mb, u.path.strip("/"), unquote(u.username), close=mb.close)
