# Message broker

Nodes that cannot accept inbound connections can still exchange messages. Every node makes outbound HTTP calls to a **broker**. The broker holds frames in queues and hands them to the node they are addressed to.

The design and the glossary are in [docs/spec.md](docs/spec.md). The HTTP endpoints are in [docs/openapi.yaml](docs/openapi.yaml).

Requirements: Python 3.10 or newer.

## Layout

| Path (`src/message_broker/`) | What it is | State |
|---|---|---|
| `frames.py`, `errors.py`, `types.py` | Frame format, error codes, shared types. | done |
| `broker/core.py` | Sessions, credentials, limits, lease. All the policy. | done |
| `broker/store.py` | The store interface and the in-memory store. | done |
| `broker/server.py` | The broker's HTTP API. | done |
| `client/http.py` | HTTP client for a broker, and `connect()`. | done |
| `client/endpoint.py` | A node's RPC calls over a connection. | done |
| `client/launcher.py` | The launcher role: register, mint, heartbeat, wait for nodes, revoke. | done |
| `adapters/flower.py` | Flower adapter: `hub` beside the SuperLink, `spoke` beside each SuperNode. | done |
| Stream channels | For byte tunnels (FLARE). | not started |
| FLARE adapter | Needs stream channels. | not started |
| Other stores (NATS, ...) | | not started |

## Run a broker

1. Install: `pip install .`
2. Copy `env.example` to `.env` and set a long random `MASTER_SECRET`.
3. Load `.env` into the environment and start: `python -m message_broker.broker`.
4. Put a reverse proxy in front of it. `nginx.conf.example` is a template. It limits `POST /sessions` to 10 requests per minute per address.

| Variable | Default | Meaning |
|---|---|---|
| `MASTER_SECRET` | required | Secret that permits `POST /sessions`. |
| `HOST` | `127.0.0.1` | Address to listen on. |
| `PORT` | `8080` | Port to listen on. |
| `RUN_TTL` | `21600` | Hard limit in seconds on a session. Heartbeats do not extend it. |
| `LEASE_SECONDS` | `120` | Seconds a session lives without a heartbeat. `0` turns the lease off. |
| `BROKER_<NAME>` | see `Limits` | Any other limit, for example `BROKER_QUEUE_BYTES`, `BROKER_SENDER_SHARE`, `BROKER_ACK_TIMEOUT`. |

The default store keeps everything in memory in one process. A restart ends every session.

## Run the Flower adapters

The launcher gives each node a connection string (`BROKER_URL`). Each adapter exits with status 0 when the session ends.

| Adapter | Command | Settings |
|---|---|---|
| Beside the SuperLink | `python -m message_broker.adapters.flower hub` | `BROKER_URL`, `SUPERLINK` (default `http://127.0.0.1:8000`), `POLLERS` (default 16) |
| Beside each SuperNode | `python -m message_broker.adapters.flower spoke` | `BROKER_URL`, `LISTEN_PORT`, `LISTEN_HOST` (default `127.0.0.1`) |

Point each SuperNode at its adapter: `flower-supernode --insecure --superlink 127.0.0.1:<LISTEN_PORT>`.

## Use it from Python

```python
from message_broker import Endpoint, Reply, connect

conn = connect("https://<credential>@broker.example.com/<session_id>")  # or set BROKER_URL
endpoint = Endpoint(conn, handler)      # handler(src, payload, meta) -> Reply, on the answering side
await endpoint.start()
reply = await endpoint.call("hub", b"request", {"k": "v"})   # on the calling side
```

The launcher registers a session, mints one credential per node and hands each node its connection string. See spec section 4.

## Tests

`pip install -e ".[test]"` then `pytest`.

- `tests/test_api_conformance.py` runs against a broker called directly and over HTTP.
- `tests/test_store.py` runs against every store. To add a store, add it to `STORES`.
- `tests/test_endpoint.py` injects faults (duplicates, lost replies) and checks that every call runs once.
- `tests/test_http.py` covers the HTTP layer and checks `docs/openapi.yaml` against the real routes.
- `tests/test_flower_adapter.py` runs the adapter against a fake SuperLink.
- `tests/test_flower_e2e.py` trains a real Flower app through a real broker (about a minute). It is skipped unless Flower is installed: `pip install -e ".[e2e]"`.
- `tests/test_flower_faults.py` does the same with duplicated and lost traffic, and with forced SuperNode retries of `push-messages`.

## Measure it

`python bench/load.py --spokes 32 --upload-mb 20` runs a broker and many nodes and prints heartbeat latency, transfer time and broker memory. `--hub-rate 10` limits the hub's link in MB/s, shared by the answers in transit. `--hub-pollers N` sets the hub's concurrent receives. `--lose-recv 0.05` loses that share of the hub's answers. `--phases baseline` runs only some phases. `--env BROKER_QUEUE_BYTES=8388608` changes a broker limit (any `Limits` field works as `BROKER_<NAME>`). Results are in spec section 12.1.
