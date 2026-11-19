# Message broker

Nodes that cannot accept inbound connections can still exchange messages. Every node makes outbound HTTP calls to a **broker**. The broker holds frames in queues and hands them to the node they are addressed to.

The design and the glossary are in [docs/spec.md](docs/spec.md). The HTTP endpoints are in [docs/openapi.yaml](docs/openapi.yaml). Measurements are in [docs/measurements.md](docs/measurements.md).

Requires Python 3.10 or newer.

## How it works

1. A **launcher** registers a **session** with the broker (it needs the master secret) and mints one credential per node. Each node gets a connection string, `https://<credential>@broker.example.com/<session_id>`.
2. A node **sends** a frame to another node's address, long-polls (**recv**) its own queue, and **acks** what it received. A frame that is not acked within `ack_timeout` is delivered again, so delivery is at least once. A full queue answers `queue_full` and the sender backs off.
3. The `Endpoint` builds RPC on top: calls and replies, large messages split into frames, duplicates dropped.
4. An **adapter** maps an application protocol onto endpoints. The Flower adapter puts a `hub` beside the SuperLink and a `spoke` beside each SuperNode.
5. A session ends when the launcher revokes it, when its lease lapses (no renewal in time), or when its time limit is reached.

## Layout

| Path (`src/message_broker/`) | What it is |
|---|---|
| `frames.py`, `errors.py`, `types.py` | Frame format, error codes, shared types. |
| `broker/core.py` | Sessions, credentials, limits, lease. All the policy. |
| `broker/ledger.py` | What is queued, and the limits and expiry that follow from it. |
| `broker/store.py` | The store interface and the in-memory store. |
| `broker/server.py` | The broker's HTTP API. |
| `client/http.py` | HTTP client for a broker, and `connect()`. |
| `client/endpoint.py` | A node's RPC calls over a connection. |
| `client/launcher.py` | The launcher role: register, mint, renew the lease, wait for nodes, revoke. |
| `adapters/flower.py` | Flower adapter: `hub` and `spoke`. |

Scope: the Flower adapter is the only adapter, and the only store is in memory. Stream channels, a FLARE adapter and external stores (NATS, ...) are not implemented.

## Run a broker

1. Install: `pip install .`
2. Copy `env.example` to `.env` and set a long random `MASTER_SECRET`.
3. Load it and start: `set -a; . ./.env; set +a; python -m message_broker.broker`. Run it under a service manager that restarts it.
4. Put a reverse proxy in front. The broker speaks plain HTTP and credentials travel in headers, so the proxy must terminate TLS. It must also:
   - not buffer responses (`recv` is a long poll)
   - accept request bodies above the largest frame, 4 MiB
   - allow a read timeout above 30 s, the longest long poll

   `nginx.conf.example` does all of this and limits `POST /sessions` to 10 requests per minute per address.

| Variable | Default | Meaning |
|---|---|---|
| `MASTER_SECRET` | required | Secret that permits `POST /sessions`. |
| `HOST` | `127.0.0.1` | Address to listen on. |
| `PORT` | `8080` | Port to listen on. |
| `BROKER_<NAME>` | see `Limits` in `broker/core.py` | Any limit. For example `BROKER_SESSION_TTL` (default 6 hours, in seconds), `BROKER_LEASE` (seconds without a renewal, default 120; `0` turns the lease off), `BROKER_MAX_NODES` (nodes per session, default 64), `BROKER_QUEUE_BYTES`, `BROKER_SENDER_SHARE`, `BROKER_ACK_TIMEOUT`. |

The default store keeps everything in memory in one process, which uses at most one core. A restart ends every session. Throughput and memory figures are in [docs/measurements.md](docs/measurements.md).

## Run the Flower adapters

The launcher gives each node a connection string (`BROKER_URL`). Each adapter exits with status 0 when the session ends.

| Adapter | Command | Settings |
|---|---|---|
| Beside the SuperLink | `python -m message_broker.adapters.flower hub` | `BROKER_URL`, `SUPERLINK` (default `http://127.0.0.1:8000`), `POLLERS` (receives at once, default 4), `SUPERLINK_CALLS` (calls to the SuperLink at once, default 16) |
| Beside each SuperNode | `python -m message_broker.adapters.flower spoke` | `BROKER_URL`, `LISTEN_PORT`, `LISTEN_HOST` (default `127.0.0.1`), `POLLERS` (receives at once, default 2) |

Point each SuperNode at its adapter: `flower-supernode --insecure --superlink 127.0.0.1:<LISTEN_PORT>`.

## Use it from Python

The launcher side registers a session, mints the credentials, renews the lease, and revokes the session on exit:

```python
from message_broker import Launcher

async with Launcher("https://broker.example.com", MASTER_SECRET, ["hub", "s0", "s1"]) as launch:
    url = launch.connection_string("s0")   # give each node its own
```

The node side:

```python
from message_broker import Endpoint, Reply, connect

conn = connect(url)                     # or set BROKER_URL
endpoint = Endpoint(conn, handler)      # handler(src, payload, meta) -> Reply, on the answering side
await endpoint.start()
reply = await endpoint.call("hub", b"request", {"k": "v"})   # on the calling side
```

## Tests

`pip install -e ".[test]"` then `pytest`. The Flower end-to-end tests are skipped unless Flower is installed: `pip install -e ".[e2e]"`.

## Measure it

`python bench/load.py --spokes 32 --upload-mb 20` starts a broker and many nodes, and prints heartbeat latency, transfer time and broker memory. `--help` lists the options, including `--url` to test a broker that is already running. Results are in [docs/measurements.md](docs/measurements.md).
