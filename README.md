# Message broker

The broker lets Flower SuperNodes reach a SuperLink that has no inbound network access. The broker is the only listener. The SuperLink side makes outbound calls only.

Requirements: Python 3.10 or newer, and the packages in `requirements.txt`.

## How it works

Three parts take part in a run:

- **SuperNode**: sends Fleet API requests to the broker.
- **Broker**: holds each request until the wrapper replies.
- **Wrapper**: runs beside the SuperLink. It polls the broker, replays each request against the local SuperLink, and sends the response back.

The broker identifies every caller by the first label of the `Host` header: `<token>.<base-domain>`. A token is lowercase hex, 16 to 63 characters.

There are two kinds of token:

- **Server token**: made by `/register`. It belongs to the wrapper and the run owner. Its requests also need the `X-Run-Secret` header.
- **Client token**: made by `/mint`. It belongs to one SuperNode. It can send only the Fleet paths in the allowlist.

A run ends when `/revoke` is called or when `RUN_TTL` seconds pass. At that time the broker removes all tokens of the run.

The broker keeps all state in memory in one process.

## Configuration

Set these environment variables. `env.example` is a template.

| Variable | Default | Meaning |
|---|---|---|
| `BASE_DOMAIN` | required | Base domain. A token is the first label before it. |
| `MASTER_SECRET` | required | Secret that permits `/register`. |
| `HOST` | `127.0.0.1` | Address to listen on. |
| `PORT` | `8080` | Port to listen on. |
| `RUN_TTL` | `21600` | Hard limit in seconds on a run. Heartbeats do not extend it. |
| `VERBOSE` | unset | Set to `1` to log every request. Lifecycle events and auth failures always log. |
| `LEASE_SECONDS` | `120` | Seconds a run lives without `POST /heartbeat`. `0` turns the lease off. |

## Deploy

1. Create a wildcard DNS record for `*.<base-domain>` that points to the broker host.
2. Get a wildcard TLS certificate for `*.<base-domain>`.
3. Copy `nginx.conf.example` to the nginx configuration. Set the domain and the certificate paths.
4. Copy `env.example` to `.env`. Set `BASE_DOMAIN` and a long random `MASTER_SECRET`.
5. Install the dependencies: `pip install -r requirements.txt`.
6. Load `.env` into the environment and start the broker: `python broker.py`.

The nginx configuration forwards the `Host` header, because the broker reads the token from it. It limits `/register` to 10 requests per minute per address (burst 5), so a guessing attack shows up as `429` lines in the nginx error log and cannot flood the broker log. It also sets `X-Forwarded-For` to the real client address, which the broker logs. It also allows request bodies up to 1 GB, turns off buffering, and sets a read timeout above the broker timeouts.

## Server endpoints

Send these to `<server-token>.<base-domain>`. Every endpoint except `/register` needs the header `X-Run-Secret: <run_secret>`.

| Method and path | Request | Response |
|---|---|---|
| `POST /register` | Header `X-Master-Secret`. | `200` with `{"run_secret": "..."}`. `403` for a wrong secret. `409` if the token exists. |
| `POST /mint` | JSON `{"n": 1..64}`. | `{"tokens": [...]}`. Each token is a client token. |
| `GET /status` | None. | JSON map of client token to `true` or `false`. A value is `true` after that SuperNode registers. |
| `POST /revoke` | None. | `204`. The run ends. Also `204` if the run is already gone. |
| `GET /next` | None. | Waits up to 25 seconds. `204` if no work. Otherwise `200` with the request body and the headers that follow. |
| `POST /reply/{rid}` | Response body. Headers `X-Status` and `X-Resp-Headers` (JSON). | `204`. |
| `POST /heartbeat` | None. | `200` with `{"remaining": seconds}`. Renews the lease. `403` if the run is gone. |
| `GET /stats` | None. | Request count and average milliseconds for each path. |

Headers on a `/next` response:

- `X-Req-Id`: the ID to use in `/reply/{rid}`.
- `X-Req-Path`: the Fleet path to replay.
- `X-Req-Headers`: the request headers, as JSON.

## Client endpoints

Send `POST` requests to `<client-token>.<base-domain>`. The broker accepts only these paths. Any other path gets `404`.

```
/v1/fleet/register-node
/v1/fleet/activate-node
/v1/fleet/deactivate-node
/v1/fleet/unregister-node
/v1/fleet/send-node-heartbeat
/v1/fleet/pull-messages
/v1/fleet/push-messages
/v1/fleet/get-run
/v1/fleet/get-fab
/v1/fleet/push-object
/v1/fleet/pull-object
/v1/fleet/confirm-message-received
```

Error responses:

| Status | Cause |
|---|---|
| `404` | Unknown token, or a path that is not in the allowlist. |
| `503` | The wrapper has not polled `/next` for more than 30 seconds. |
| `504` | The wrapper did not reply within 60 seconds. |

## Liveness

`GET /alive` needs no secret. Send it to any server or client token host. It does not renew the lease.

| Status | Meaning |
|---|---|
| `200` | `{"remaining": seconds}`. The run is live. |
| `410` | The run expired, was revoked, or the token is unknown. |
| `404` | The host has no valid token label. |

Test: `python smoke_test.py`.
