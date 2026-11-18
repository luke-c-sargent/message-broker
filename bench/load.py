"""Load harness for the broker. Not a test: it prints numbers.

Starts a broker process, then N spoke nodes and one hub node in this process. Phases:
  baseline   every spoke sends a small RPC ("heartbeat") each second
  upload     every spoke uploads --upload-mb at once, heartbeats continue
  download   every spoke fetches --upload-mb from the hub at once, heartbeats continue
Reports heartbeat latency against Flower's 5 s budget, transfer time and the broker's memory.

  python bench/load.py --spokes 32 --upload-mb 20
  python bench/load.py --spokes 32 --upload-mb 20 --env BROKER_SENDER_SHARE=1.0   # without the sender share
"""

import argparse
import asyncio
import os
import random
import statistics
import subprocess
import sys
import time

from message_broker import Endpoint, Launcher, Reply, connect
from message_broker.types import MiB

HEARTBEAT_BUDGET = 5.0  # seconds Flower gives send-node-heartbeat (spec 11.1)


def rss_mb(pid: int) -> float:
    out = subprocess.run(["ps", "-o", "rss=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return int(out) / 1024 if out else 0.0


async def until_up(port: int) -> None:
    for _ in range(100):
        try:
            _, w = await asyncio.open_connection("127.0.0.1", port)
            w.close()
            return
        except OSError:
            await asyncio.sleep(0.1)
    raise SystemExit("broker did not start")


def cpu_s(pid: int) -> float:
    """CPU seconds the process has used so far (ps prints [h:]mm:ss.cc)."""
    out = subprocess.run(["ps", "-o", "time=", "-p", str(pid)], capture_output=True, text=True).stdout.strip()
    return sum(float(x) * 60 ** i for i, x in enumerate(reversed(out.split(":")))) if out else 0.0


def pct(xs: list[float], p: float) -> float:
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p))] if xs else float("nan")


class Throttled:
    """A Connection whose recv answers arrive over a link of `rate` MB/s shared fairly by the answers in transit
    (like TCP): one answer alone gets the whole link, n at once get a share each. An answer reaches the caller
    only when it has fully arrived, as with a real HTTP response."""

    def __init__(self, conn, rate):
        self.conn, self.rate, self.active = conn, rate, 0

    async def info(self):
        return await self.conn.info()

    async def send(self, dest, frame, ttl=None):
        await self.conn.send(dest, frame, ttl)

    async def recv(self, wait):
        got = await self.conn.recv(wait)
        size, sent = sum(len(d.frame) for d in got), 0
        self.active += 1
        try:
            while sent < size:
                step = min(256 * 1024, size - sent)
                await asyncio.sleep(step / MiB / (self.rate / self.active))
                sent += step
        finally:
            self.active -= 1
        return got

    async def ack(self, ids):
        await self.conn.ack(ids)


class Lossy:
    """A Connection whose recv answers are lost with probability `p` after the broker handed the frames over:
    they stay unacked and the broker must redeliver them (spec 7.1). Stands in for a flaky network."""

    def __init__(self, conn, p):
        self.conn, self.p, self.lost = conn, p, 0

    async def info(self):
        return await self.conn.info()

    async def send(self, dest, frame, ttl=None):
        await self.conn.send(dest, frame, ttl)

    async def recv(self, wait):
        got = await self.conn.recv(wait)
        if got and random.random() < self.p:
            self.lost += 1
            raise OSError("answer lost")
        return got

    async def ack(self, ids):
        await self.conn.ack(ids)


class Phase:
    def __init__(self, name):
        self.name, self.beats, self.fails, self.xfer, self.xfer_fail, self.t0, self.t1 = name, [], 0, [], 0, 0, 0
        self.cpu = 0.0  # broker CPU seconds used during the phase

    def report(self, rss_peak: float, mb_moved: float) -> None:
        b = self.beats
        over = sum(1 for x in b if x > HEARTBEAT_BUDGET)
        line = f"{self.name:9} beats={len(b):4} p50={pct(b, .5)*1000:7.0f}ms p99={pct(b, .99)*1000:7.0f}ms " \
               f"max={max(b, default=0)*1000:7.0f}ms over5s={over} failed={self.fails} " \
               f"broker_cpu={100 * self.cpu / max(self.t1 - self.t0, 1e-9):.0f}%"
        if self.xfer or self.xfer_fail:
            line += (f" | transfers ok={len(self.xfer)} failed={self.xfer_fail} "
                     f"median={statistics.median(self.xfer):.1f}s max={max(self.xfer):.1f}s "
                     f"rate={mb_moved / (self.t1 - self.t0):.0f}MB/s broker_rss_peak={rss_peak:.0f}MB") if self.xfer else ""
        print(line, flush=True)


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--spokes", type=int, default=32)
    ap.add_argument("--upload-mb", type=int, default=20)
    ap.add_argument("--baseline-s", type=float, default=8)
    ap.add_argument("--beat-s", type=float, default=1.0)
    ap.add_argument("--port", type=int, default=18700)
    ap.add_argument("--hub-rate", type=float, default=0, help="cap the hub's receive rate in MB/s (a slow link to the hub)")
    ap.add_argument("--backoff-max", type=float, default=None, help="longest wait after queue_full, seconds (default: the Endpoint's)")
    ap.add_argument("--hub-pollers", type=int, default=16, help="concurrent recv calls of the hub")
    ap.add_argument("--lose-recv", type=float, default=0, help="chance the hub's recv answer is lost (a flaky link)")
    ap.add_argument("--phases", default="baseline,upload,download", help="comma list of phases to run")
    ap.add_argument("--repeat", type=int, default=1, help="repeat the upload phase, to see whether memory keeps growing")
    ap.add_argument("--env", action="append", default=[], help="NAME=value for the broker process, repeatable")
    a = ap.parse_args()
    env = {**os.environ, "MASTER_SECRET": "m", "PORT": str(a.port), "LEASE_SECONDS": "0"}
    env.update(kv.split("=", 1) for kv in a.env)
    broker = subprocess.Popen([sys.executable, "-m", "message_broker.broker"], env=env,
                              stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    lag = [0.0]
    big = b"x" * (a.upload_mb * MiB)

    async def hub_handler(src, payload, meta):
        if meta.get("op") == "download":
            return Reply(big)
        return Reply(b"ok")  # beat and upload: answer at once (the real SuperLink time is not measured here)

    async def lag_monitor():
        while True:
            t = time.perf_counter()
            await asyncio.sleep(0.05)
            lag[0] = max(lag[0], time.perf_counter() - t - 0.05)
    lagger = None
    try:
        await until_up(a.port)
        addrs = ["hub"] + [f"s{i}" for i in range(a.spokes)]
        async with Launcher(f"http://127.0.0.1:{a.port}", "m", addrs) as launch:
            conns = {x: connect(launch.connection_string(x)) for x in addrs}
            hub_conn = Throttled(conns["hub"], a.hub_rate) if a.hub_rate else conns["hub"]
            hub_conn = Lossy(hub_conn, a.lose_recv) if a.lose_recv else hub_conn
            hub = Endpoint(hub_conn, hub_handler, pollers=a.hub_pollers)
            extra = {"backoff": (0.05, a.backoff_max)} if a.backoff_max else {}
            spokes = [Endpoint(conns[f"s{i}"], pollers=4, **extra) for i in range(a.spokes)]
            await asyncio.gather(hub.start(), *(s.start() for s in spokes))
            lagger = asyncio.ensure_future(lag_monitor())
            print(f"{a.spokes} spokes, {a.upload_mb} MB per transfer, broker env {a.env or 'default'}", flush=True)

            async def beats(ph: Phase, stop: asyncio.Event):
                async def one(ep):
                    while not stop.is_set():
                        t = time.perf_counter()
                        try:
                            await ep.call("hub", b"beat", {"op": "beat"}, 25)
                            ph.beats.append(time.perf_counter() - t)
                        except Exception:
                            ph.fails += 1
                        await asyncio.sleep(a.beat_s)
                await asyncio.gather(*(one(s) for s in spokes))

            async def phase(name, work=None):
                ph, stop, peak = Phase(name), asyncio.Event(), [0.0]

                async def sample():
                    while not stop.is_set():
                        peak[0] = max(peak[0], rss_mb(broker.pid))
                        await asyncio.sleep(0.25)
                bt, sm = asyncio.ensure_future(beats(ph, stop)), asyncio.ensure_future(sample())
                ph.t0, cpu0 = time.perf_counter(), cpu_s(broker.pid)
                if work:
                    async def one(ep):
                        t = time.perf_counter()
                        try:
                            await work(ep)
                            ph.xfer.append(time.perf_counter() - t)
                        except Exception as e:
                            ph.xfer_fail += 1
                            print(f"  transfer failed: {e!r}"[:160])
                    await asyncio.gather(*(one(s) for s in spokes))
                else:
                    await asyncio.sleep(a.baseline_s)
                ph.t1, ph.cpu = time.perf_counter(), cpu_s(broker.pid) - cpu0
                await asyncio.sleep(a.beat_s * 1.5)  # let the last beats finish
                stop.set()
                await asyncio.gather(bt, sm)
                ph.report(peak[0], a.spokes * a.upload_mb)

            want = a.phases.split(",")
            if "baseline" in want:
                await phase("baseline")
            for i in range(a.repeat if "upload" in want else 0):
                await phase("upload", lambda ep: ep.call("hub", big, {"op": "upload"}, 120))
                await asyncio.sleep(3)
                print(f"          broker rss after the burst and 3 s idle: {rss_mb(broker.pid):.0f} MB", flush=True)
            if "download" in want:
                await phase("download", lambda ep: ep.call("hub", b"get", {"op": "download"}, 120))
            if a.lose_recv:
                print(f"answers lost on the hub link: {hub_conn.lost}")
            print(f"event-loop lag in the load generator: max {lag[0]*1000:.0f} ms "
                  f"(if this is high, the generator, not the broker, is the limit)")
            for e in [hub, *spokes]:
                await e.close()
            for c in conns.values():
                await c.close()
    finally:
        if lagger:
            lagger.cancel()
        broker.terminate()
        broker.wait(10)


asyncio.run(main())
