"""A node's data-plane link to a broker: the interface an Endpoint uses."""

from typing import Protocol

from ..types import Delivery, Info


class Connection(Protocol):
    async def info(self) -> Info: ...
    async def send(self, dest: str, frame: bytes, ttl: float | None = None) -> None: ...
    async def recv(self, wait: float) -> list[Delivery]: ...
    async def ack(self, ids: list[int]) -> None: ...


class BoundConnection:
    """A Connection for one credential, over any backend whose data operations take (sid, cred, ...)."""

    def __init__(self, backend, sid: str, cred: str, close=None):
        self.backend, self.sid, self.cred, self._close = backend, sid, cred, close

    async def info(self) -> Info:
        return await self.backend.info(self.sid, self.cred)

    async def send(self, dest: str, frame: bytes, ttl: float | None = None) -> None:
        await self.backend.send(self.sid, self.cred, dest, frame, ttl)

    async def recv(self, wait: float) -> list[Delivery]:
        return await self.backend.recv(self.sid, self.cred, wait)

    async def ack(self, ids: list[int]) -> None:
        await self.backend.ack(self.sid, self.cred, ids)

    async def close(self) -> None:
        if self._close:
            await self._close()
