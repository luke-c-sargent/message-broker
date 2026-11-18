"""The launcher role (spec section 2): registers a session, mints node credentials, renews the lease,
ends the session. It does not start processes: that is the orchestrator's job."""

import asyncio
from urllib.parse import quote, urlsplit

from ..errors import BrokerError
from ..types import MINUTES, SECONDS
from .http import BrokerClient


class Launcher:
    """Use as `async with Launcher(url, secret, ["hub", "c1"]) as launch:`. The session ends on exit."""

    def __init__(self, base_url: str, master_secret: str, addresses: list[str], beat: float = 30 * SECONDS):
        self.base, self.secret, self.addresses, self.beat = base_url.rstrip("/"), master_secret, addresses, beat
        self.client = BrokerClient(self.base)
        self.gone = asyncio.Event()  # set when the broker says the session is over
        self.sid = self.admin = None
        self._creds: dict[str, str] = {}
        self._task = None

    async def __aenter__(self) -> "Launcher":
        self.sid, self.admin = await self.client.register(self.secret)
        self._creds = await self.client.mint(self.sid, self.admin, self.addresses)
        self._task = asyncio.ensure_future(self._heartbeat())
        return self

    async def __aexit__(self, *exc) -> None:
        self._task.cancel()
        try:
            await self.client.revoke(self.sid, self.admin)
        except BrokerError:
            pass  # the session may already be gone
        await self.client.close()

    async def _heartbeat(self) -> None:
        while True:
            await asyncio.sleep(self.beat)
            try:
                await self.client.heartbeat(self.sid, self.admin)
            except BrokerError as e:
                if e.code not in ("unavailable", "queue_full"):  # a network blip is retried next beat
                    self.gone.set()
                    return

    def connection_string(self, address: str) -> str:
        """What to give the node at `address` (for example as BROKER_URL)."""
        u = urlsplit(self.base)
        return f"{u.scheme}://{quote(self._creds[address], safe='')}@{u.netloc}/{self.sid}"

    async def wait_joined(self, addresses: list[str] | None = None, timeout: float = 1 * MINUTES, every: float = 0.5 * SECONDS) -> None:
        """Wait until the nodes have joined (spec 9.2: presence). Raises BrokerError("timeout")."""
        want, end = set(addresses or self.addresses), asyncio.get_running_loop().time() + timeout
        while True:
            p = await self.client.presence(self.sid, self.admin)
            if all(p[a]["joined"] for a in want):
                return
            if asyncio.get_running_loop().time() > end:
                raise BrokerError("timeout", f"not joined: {sorted(a for a in want if not p[a]['joined'])}")
            await asyncio.sleep(every)
