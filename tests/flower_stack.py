"""A real Flower system through the broker, for the end-to-end tests: a broker process, a SuperLink, the hub
adapter, and per spoke an adapter and a SuperNode. Recorders see the traffic on both sides of the adapters;
an optional FaultProxy damages the traffic between the adapters and the broker."""

import asyncio
import os
import socket
import subprocess
import sys
import tempfile
from pathlib import Path

from faults import FaultProxy, Recorder

from message_broker import Launcher

APP = Path(__file__).parent / "e2e" / "app"
BIN = Path(sys.executable).parent


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def until_up(port: int, timeout: float = 30) -> None:
    end = asyncio.get_running_loop().time() + timeout
    while True:
        try:
            _, w = await asyncio.open_connection("127.0.0.1", port)
            w.close()
            return
        except OSError:
            if asyncio.get_running_loop().time() > end:
                raise TimeoutError(f"nothing listening on {port}")
            await asyncio.sleep(0.2)


class FlowerStack:
    def __init__(self, tmp_path: Path, spokes: int = 2, faults: dict | None = None, broker_env: dict | None = None,
                 node_lose_first: dict | None = None):
        self.tmp, self.n, self.faults, self.broker_env = tmp_path, spokes, faults, broker_env or {}
        self.node_lose_first = node_lose_first
        self.home = tempfile.mkdtemp()
        self.procs: list[subprocess.Popen] = []
        self.logs: dict = {}
        self.proxies: list = []
        self.hub = self.spokes = self.launch = self.fault = self.link_rec = None
        self.node_recs: list[Recorder] = []

    def start_proc(self, name, argv, **env) -> subprocess.Popen:
        self.logs[name] = open(self.tmp / f"{name}.log", "w+")
        p = subprocess.Popen(argv, stdout=self.logs[name], stderr=subprocess.STDOUT, env={
            **os.environ, "PATH": f"{BIN}{os.pathsep}{os.environ['PATH']}", "FLWR_HOME": f"{self.home}/{name}", **env})
        self.procs.append(p)
        return p

    def tail(self, name: str, n: int = 1500) -> str:
        self.logs[name].flush()
        return Path(self.logs[name].name).read_text()[-n:]

    async def start(self) -> "FlowerStack":
        bport, lport = free_port(), free_port()
        self.start_proc("broker", [sys.executable, "-m", "message_broker.broker"], MASTER_SECRET="m",
                        PORT=str(bport), LEASE_SECONDS="0", **self.broker_env)
        await until_up(bport)
        broker = f"http://127.0.0.1:{bport}"
        self.launch = await Launcher(broker, "m", ["hub"] + [f"c{i}" for i in range(self.n)]).__aenter__()
        nodes_url = broker  # where adapters connect: the broker, or the fault proxy in front of it
        if self.faults is not None:
            self.fault = FaultProxy(broker, **self.faults)
            nodes_url = f"http://127.0.0.1:{await self.fault.start()}"
            self.proxies.append(self.fault)

        def conn(addr: str) -> str:
            return self.launch.connection_string(addr).replace(broker.removeprefix("http://"), nodes_url.removeprefix("http://"))

        Path(self.home, "super").mkdir()
        Path(self.home, "super/config.toml").write_text(
            f'[superlink]\ndefault = "poc"\n\n[superlink.poc]\naddress = "127.0.0.1:{lport}"\ninsecure = true\n')
        self.start_proc("superlink", [str(BIN / "flower-superlink"), "--insecure", "--port", str(lport),
                                      "--disable-runtime-dependency-installation"])
        await until_up(lport)
        self.link_rec = Recorder(f"http://127.0.0.1:{lport}")  # what the SuperLink really receives
        self.proxies.append(self.link_rec)
        self.hub = self.start_proc("hub", [sys.executable, "-m", "message_broker.adapters.flower", "hub"],
                                   BROKER_URL=conn("hub"), SUPERLINK=f"http://127.0.0.1:{await self.link_rec.start()}")
        self.spokes = []
        for i in range(self.n):
            port = free_port()
            self.spokes.append(self.start_proc(f"spoke{i}", [sys.executable, "-m", "message_broker.adapters.flower",
                                                             "spoke"], BROKER_URL=conn(f"c{i}"), LISTEN_PORT=str(port)))
            await until_up(port)
            rec = Recorder(f"http://127.0.0.1:{port}", self.node_lose_first)  # what the SuperNode really sends
            self.node_recs.append(rec)
            self.proxies.append(rec)
            self.start_proc(f"node{i}", [str(BIN / "flower-supernode"), "--insecure", "--superlink",
                                         f"127.0.0.1:{await rec.start()}", "--node-config",
                                         f"partition-id={i} num-partitions={self.n}", "--port", str(free_port())])
        await self.launch.wait_joined(timeout=60)
        return self

    async def run_app(self, timeout: float = 300) -> str:
        """flwr run the app. Returns its output; raises AssertionError with logs if it failed."""
        run = await asyncio.to_thread(
            subprocess.run, [str(BIN / "flwr"), "run", str(APP), "poc", "--stream"], capture_output=True,
            text=True, timeout=timeout, env={**os.environ, "PATH": f"{BIN}{os.pathsep}{os.environ['PATH']}",
                                             "FLWR_HOME": f"{self.home}/super"})
        out = run.stdout + run.stderr
        assert run.returncode == 0 and "Strategy execution finished" in out, (
            out[-2500:] + "\n--- hub:\n" + self.tail("hub") + "\n--- node0:\n" + self.tail("node0"))
        return out

    async def end_session(self) -> None:
        await self.launch.__aexit__(None, None, None)

    async def adapters_exit_codes(self) -> list[int]:
        return [await asyncio.to_thread(p.wait, 15) for p in [self.hub] + self.spokes]

    async def close(self) -> None:
        for p in reversed(self.procs):
            p.terminate()
        for p in self.procs:
            try:
                p.wait(10)
            except subprocess.TimeoutExpired:
                p.kill()
        for x in self.proxies:
            try:
                await x.stop()
            except Exception:
                pass
