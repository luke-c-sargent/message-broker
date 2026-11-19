# Message broker: wire and architecture specification

Status: **draft 0.5, provisional.** Parameters marked *provisional* are defaults to be confirmed (section 12).
This document is language-neutral. It says what the parts must do, not how a language does it. Section 2 defines the words it uses.

## 1. Purpose and rules

A set of nodes that cannot accept inbound connections must exchange messages. A **broker** that every node can reach carries the messages.

Rules that hold for the whole design:

1. **Outbound only.** Every connection is started by a launcher or a node toward the broker. The broker never starts a connection to anyone.
2. **One HTTP API.** All communication with a broker is HTTP. Whoever runs the broker chooses the store behind it. Nodes never see it.
3. **The broker is dumb about protocols.** It moves opaque frames from one node address to another. It does not read payloads. It does not know any application protocol, and it has no idea which node plays which application role.
4. **Protocol work lives in adapters.** An adapter turns one application protocol (Flower, FLARE, others) into channels. The adapters decide who plays which role. They also enforce policy: for example an allowlist of calls, and which senders to accept. Nodes are untrusted.
5. **Clean break.** This specification replaces the old Host-header token API. There is no compatibility layer.

## 2. Terms

This glossary is the authority for the words in this specification.

| Term | Meaning |
|---|---|
| **Broker** | A long-running service that carries frames between the nodes of a session, over an HTTP API. It enforces credentials, limits and the lease, keeps its queues in a store, and never starts a connection. |
| **Store** | The pluggable storage behind a broker: named queues. Nodes never see it. The default is in memory. |
| **Session** | One use of a broker by a group of nodes, from registration to teardown. A namespace with its own lease, credentials and queues. |
| **Launcher** | A role, not a program. It registers a session, mints node credentials, produces each node's connection string, renews the lease and ends the session. It may share a process with a node. |
| **Orchestrator** | Whatever starts the launcher and the node processes and delivers the connection strings to them (a workflow system, a job scheduler, a shell script). Outside this project. |
| **Node** | One participant in a session: on one machine, an application process, its adapter and its endpoint. It has one address and one node credential. |
| **Address** | A node's name in a session, chosen by the launcher and unique in that session, for example `hub` or `c0`. A name, not the node. |
| **Credential** | A secret sent as a bearer token. Three kinds (section 4.3): the **master secret** registers sessions, the **admin credential** is the launcher's for one session, and a **node credential** is one node's. |
| **Connection string** | What a node is given to join: its node credential, the broker's location and the session id. |
| **Application** | The system whose traffic is carried (Flower, FLARE). An **application process** is one of its programs (a Flower SuperLink or SuperNode, a FLARE server or client). The application keeps its own role names. This specification uses "client" and "server" for nothing else. |
| **Adapter** | Code on the same machine as one application process. It speaks the application's protocol to it locally, turns its traffic into messages and enforces policy (allowlists, accepted senders). |
| **Hub** | The node whose application process every other node's application process talks to (the Flower SuperLink, the FLARE server). Its adapter is the hub adapter. By convention its address is `hub`. "The hub" is the node, and `hub` is its address. |
| **Spoke** | Any node other than the hub. Its adapter, the spoke adapter, sends only to the hub and drops frames from other senders. Hub and spoke apply only to an application with one such central process. |
| **Frame** | The unit the broker carries: a header and a payload (section 5). |
| **Message** | What an adapter hands its endpoint, such as one Flower request or reply. It is cut into frames and put together again by the receiving endpoint. A stream channel carries bytes, not messages. |
| **Channel** | One conversation between two nodes: RPC (one request, one reply) or stream (ordered bytes both ways). |
| **Endpoint** | The part of a node that speaks the frame protocol: it opens and answers channels, cuts messages into frames, and acknowledges, orders and de-duplicates received frames. It keeps several `recv` calls open, and each loop is a **poller**. |
| **Lane** | One of an address's two queues: small frames (heartbeats, control calls) and bulk frames (section 7.4). |
| **Lease** | How long a session may go without a **lease renewal**, which is the launcher's `renew` call. When it runs out, the session ends. |
| **Session TTL** | A hard limit on a session's life, counted from registration. Lease renewals do not extend it. |
| **Frame TTL** | How long a frame may wait in a queue before the broker drops it (`ttl` on `send`, at most `frame_ttl`). |
| **Heartbeat** | A small periodic message the application sends to show a node is alive (Flower's `send-node-heartbeat`, FLARE's PING). To the broker it is an ordinary small frame. Not the lease renewal. |
| **Presence** | Which nodes have joined (sent or received at least once), and when each was last seen. |

## 3. Topology

The broker has no topology. Any node can send to any other address in the same session. A node can read only its own address. An adapter makes a hub-and-spoke exchange, a pipeline or anything else by choosing whom it sends to and whose frames it accepts.

```
node c1 ──┐                        ┌── node hub
node c2 ──┼──►  broker (session) ◄─┤
node c3 ──┘                        └── launcher (admin)
   all arrows are connections started toward the broker
```

An adapter MUST ignore (drop) a frame whose `src` it does not expect. Example: an adapter on a spoke node accepts frames only from `hub`.

## 4. Session lifecycle and credentials

### 4.1 Lifecycle

1. The launcher calls `register` with the **master secret**. The broker creates a session and returns the `session_id` and an **admin credential**.
2. The launcher calls `mint` with the admin credential and a list of addresses. The broker returns one **node credential** for each address. A session holds at most `max_nodes` nodes (*provisional*: 64). `mint` may be called again later to add nodes (late joiners, replacements). If the reply to a `mint` is lost, the launcher repeats the call. An address that has not yet joined (section 9.2) is minted again with a new credential, and the old credential stops working. An address that has already joined returns `conflict`. A `mint` is all or nothing. It returns `bad_frame` for an invalid or duplicate address, or if it would pass `max_nodes`.
3. The launcher gives each node a connection string that carries its own credential. How the string reaches a process is the orchestrator's job (an environment variable, a job parameter, a file). Each adapter is also told its own address and its peers' addresses by the orchestrator.
4. Nodes exchange frames. The launcher renews the lease with the `renew` operation.
5. The session ends when any of these happens: `revoke` is called; the lease lapses; the session TTL is reached. When a session ends, the broker deletes its credentials and queues. Every channel is closed. Pending `recv` calls return `session_gone`.

`revoke` is idempotent. After the session is gone, an expired, revoked and never-existing session all look the same to a caller.

An address is 1 to 64 characters from `A-Z a-z 0-9 _ -`. This keeps addresses safe in URLs and in store keys.

### 4.2 Lease and session TTL

- **Lease:** the session lives `lease` seconds after the last lease renewal. `lease = 0` turns the lease off.
- **Session TTL:** a hard limit in seconds on the session, counted from registration. Lease renewals do not extend it.
- Only the admin credential may call `renew`.
- The lease detects that the launcher is gone (a killed task) and stops the session from leaking. Whether the loss of a particular node should end the session is a policy for the launcher. The launcher reads `presence` (section 9.2) and calls `revoke` when its own rule is broken, for example "the hub node has been silent for 60 seconds".

### 4.3 Credential kinds and permissions

| Credential | Held by | Allowed |
|---|---|---|
| **Master secret** | Whoever runs the broker, and the launchers it trusts | `register` only. |
| **Admin credential** | Launcher only | `mint`, `revoke`, `renew`, `presence`, `stats`, `alive`. |
| **Node credential** | One node | `alive`, `info`, `ack`; `send` to any address in its session; `recv` from its own address only. |

The broker enforces these rules for node credentials:

1. **Own mail only.** A node can read only its own address.
2. **Unforgeable sender.** The broker stamps the true sender address (`src`) on every frame. A node cannot choose it.
3. **Session only.** A node can reach only addresses of its own session.
4. **No admin.** A node cannot call any admin operation.

The broker enforces these rules, not the store. The conformance tests (section 13) check each one.

### 4.4 Credential form

- A credential is opaque, at least 128 bits of entropy, and sent as a bearer token. It goes in the `Authorization: Bearer <credential>` header. It never goes in a URL path or query that a proxy logs.
- A **connection string** carries what a node needs: `<scheme>://<credential>@<host>[:port]/<session_id>`. The scheme is `http` or `https`. The userinfo part is the credential. A process MUST strip it from the URL before it logs or connects, and send it as the bearer credential. A process reads its connection string from the `BROKER_URL` environment variable unless the caller passes one explicitly.
- Logs never contain a full credential. A short prefix is allowed.

## 5. Frame

A frame is a byte string. It has three parts in this order:

```
+------------------+---------------------+----------------+
| header length    | header              | payload        |
| 4 bytes, unsigned| UTF-8 JSON object   | 0..N raw bytes |
| big-endian       | (header length bytes)|               |
+------------------+---------------------+----------------+
```

The broker treats the whole frame as opaque, except that it enforces the size limit and may parse the header to log. A store that cannot hold raw bytes encodes the frame (for example as base64) inside its own implementation. That cost belongs to that store only.

### 5.1 Header fields

| Field | Type | Required | Meaning |
|---|---|---|---|
| `v` | integer | yes | Frame format version. This specification defines `1`. A receiver that does not know a version drops the frame. |
| `ch` | string | yes | Channel id. The opener creates it. It is unique among that opener's channels in this session and is never reused. |
| `seq` | integer | yes | Sequence number of this frame in this channel and direction. It starts at 0 and rises by 1. |
| `kind` | string | yes | `open`, `data` or `close`. |
| `mode` | string | on `open` | `rpc` or `stream` (section 6). |
| `more` | boolean | no | `true` means the message continues in the next frame (chunking). Default `false`. Used in `rpc` mode. |
| `rsp` | boolean | no | `true` on every frame sent by the responder (the side that did not open the channel). Default `false`. It gives each frame a direction, so a channel's frames are told apart even if both nodes happen to use the same `ch`. |
| `status` | string | on `close` | `ok` or an error code (section 8). |
| `meta` | object of strings | no | Adapter data (for example the path and headers of a Flower call). The endpoint does not read it. |

The sender does not put its own address in the frame. The broker adds the sender address on delivery (the `src` value of `recv`). A receiver MUST trust `src` and MUST NOT trust any address a sender wrote in `meta`.

Unknown header fields MUST be ignored. That is how the format grows without a version change.

### 5.2 Size and chunking

A broker states `max_frame` (bytes, whole frame). A sender MUST NOT send a larger frame. A message bigger than the payload space is split into several frames of the same channel and direction. Each chunk has its own `seq`. All chunks except the last have `more: true`. The receiver joins them in `seq` order. A broker rejects a frame larger than `max_frame` with `too_large`.

## 6. Channels

A channel is a conversation between two nodes. The **opener** chooses `ch` and sends `open` to the other node (the **responder**). The channel is identified by the opener's address and `ch`. Each direction has its own `seq` counter. A node answers on a channel by sending to the `src` of the frames it received.

### 6.1 RPC mode (`mode: rpc`)

One request and one response.

1. The opener sends `open` with the request payload (chunked if needed). Every frame of the request carries a `ttl` equal to the opener's deadline (at most `frame_ttl`, section 7.1). A request that waited longer than its deadline is therefore never delivered.
2. The responder sends `close` with the response payload and `status` (`ok` or an error code).

The channel is over when the `close` frame is complete. The opener sets a deadline. If it passes, the opener gives up and treats the channel as closed with `timeout`. A late `close` frame for a closed channel is dropped.

### 6.2 Stream mode (`mode: stream`)

*Design only: not implemented, not measured.*

An ordered, two-way byte stream.

1. The opener sends `open` (it may carry a first payload).
2. Either side sends `data` frames. Each frame's payload is the next bytes of that direction's stream.
3. Either side sends `close` to end its use of the channel. After `close`, the channel is closed in both directions. `status` says why.

A stream channel is closed if the session ends or if a side reports `peer_gone`.

**Coalescing.** A stream adapter does not send one frame per read from its local socket. It collects bytes and sends a frame when it holds `max_frame` bytes of payload, or when `coalesce_delay` (*provisional*: 10 ms) has passed since the first pending byte. Otherwise a stream of small reads costs one request per read.

### 6.3 Ordering and duplicates

The broker does not promise order (section 7). The receiving endpoint therefore:

- tracks the next expected `seq` per `(src, ch, direction)`;
- **drops** a frame with a `seq` lower than expected (a duplicate);
- **holds** a frame with a higher `seq` until the missing frames arrive, up to `reorder_bytes` of held payload per channel; if that limit is passed, it closes the channel with `bad_frame`;
- delivers frames to the adapter strictly in `seq` order;
- **remembers closed channels.** When a channel closes, the receiver keeps its `(src, ch)` for `2 × frame_ttl` and drops every frame for it. Without this, a late copy of an `open` (see section 7.1) would look like a new channel and run the request again. The time is enough because an opener's deadline is at most `frame_ttl` and the broker drops any frame older than `frame_ttl`.

Out-of-order delivery is normal. Several consumers may read from one address at the same time (section 7.2), and a frame whose ack was lost comes back only after `ack_timeout`. By then the sender may have delivered many later frames of a large message. The gap is therefore not limited by the number of consumers, so the limit is in bytes: `reorder_bytes` (*provisional*: 64 MiB per channel).

## 7. Delivery

### 7.1 Broker guarantee: at least once, with acknowledgement

- `send` returns success only when the broker has accepted the frame into the destination queue. A `send` may carry a `ttl` in seconds. The default and the maximum is `frame_ttl` (*provisional*: 120 s). The broker drops a frame that is still queued when its `ttl` ends, and never delivers it. The broker uses its own clock only, so nodes need no shared clock.
- `recv` returns frames, each with a `delivery_id`. The consumer calls `ack` as soon as the adapter holds the frame in its own memory. That is **receipt**, not the end of handling, and the ack is its own call, not part of the next `recv`. A frame that is not acknowledged within `ack_timeout` (*provisional*: 5 s) goes back to the queue and may be delivered again, to any consumer of that address. A short timeout is safe because an ack takes one round trip. A consumer that fails after the ack loses the frame. For an RPC the opener's deadline then ends the call, and the application retries as a new channel.
- A sender that does not know whether a `send` worked (a network error) sends the **same frame again, with the same `seq`**. The receiver's duplicate rule (section 6.3) removes the extra copy. A receiver acknowledges a duplicate too, and then drops it.

### 7.2 Competing consumers

Several `recv` calls may run at the same time on one address (an adapter may run many pollers). The broker delivers each frame to exactly one of them at a time. Order across consumers is not defined. The endpoint restores order (section 6.3).

### 7.3 What the broker does not promise

- It does not promise order. The endpoint restores it.
- It does not keep frames across a broker restart with the default store, which is in memory. After a restart, all sessions are gone. Callers see `session_gone`. The orchestrator then starts the whole exchange again.
- It does not promise that an **application** retry is a duplicate. If an application retries a call, that is a new channel with a new `ch`. The endpoint cannot remove it. Adapters that need this (Flower) rely on the application's own tolerance (section 11.1).

### 7.4 Lanes, limits and backpressure

Each address has two **lanes**: `small`, for frames up to `small_frame_bytes` (*provisional*: 64 KiB), such as application heartbeats and control calls, and `bulk`, for the rest. The broker picks the lane from the frame size. Lanes are invisible to a node, except that frames may arrive in a different order than they were sent. The endpoint already copes with that (section 6.3).

`recv` serves both lanes. It takes up to half of `recv_batch_bytes` from the small lane first, then gives the rest of the batch to the bulk lane. A lane with less than its half gives the rest to the other. So a small frame never waits behind queued bulk frames, and neither lane can starve the other. A node could cut bulk data into small frames to jump ahead. The small lane's own limits and the half-batch rule limit what that gains.

Version 1 bounds memory in these places. A frame counts against them while it is queued or delivered but not yet acknowledged.

- Each lane of each address has a limit: `queue_frames` frames, and in bytes `small_queue_bytes` (small lane) or `queue_bytes` (bulk lane).
- Each session has a total limit, `session_bytes`, across all its bulk lanes. The small lanes are bounded by their own limits and by `max_nodes`.
- Each sender has a **share** of each lane's limits: `sender_share` (*provisional*: one quarter, in frames and in bytes).

The limits are per lane so that a queue full of large frames does not refuse an application heartbeat. The share is about fairness under normal load, not only about hostile nodes. At the end of a round many spokes upload large model updates at the same time. Without a share, a few uploads fill the `hub` queue and the small control frames of other nodes (which Flower gives 5 s) get `queue_full`.

When any limit would be passed, `send` fails with `queue_full`. The sender waits (exponential back-off) and tries again. A stream adapter stops reading from its local socket while a send is waiting. That lets TCP slow the application. Credit-based flow control is not part of version 1.

**Known limit.** Any node can send to any address. A hostile node can use its whole share of every lane, and then other senders still have the rest of each lane, but the lane is smaller for them. This is a denial of service only. It does not expose data, because a node cannot read other addresses and cannot forge `src`. Request-rate limits stay in the reverse proxy.

**Redelivery must not outrun delivery.** A frame is acknowledged only after the node has received the whole `recv` answer. If that takes longer than `ack_timeout`, the broker delivers the frames again while they are still arriving, and a slow link can end in a storm of redeliveries in which nothing completes (`docs/measurements.md`, section 2). Over a link of `R` bytes per second, with `p` concurrent `recv` calls that each take up to `B` bytes, an answer needs about `p × B ÷ R` seconds. `ack_timeout` must be longer than that.

## 8. Errors

Broker operations use these abstract error codes. Section 10 maps them to HTTP.

| Code | Meaning | Caller action |
|---|---|---|
| `unauthorized` | No credential, or an unknown credential. | Stop. Do not retry. |
| `forbidden` | The credential is valid but may not do this. | Stop. Do not retry. |
| `session_gone` | The session ended (revoked, lease lapsed, session TTL, restart, or it never existed). | Stop. The session is over. |
| `unknown_dest` | The destination address is not a live node of this session. | Fix the caller, or wait for the node to be minted. |
| `conflict` | The address has already joined, so it cannot be minted again. | Fix the caller. |
| `queue_full` | A queue limit, the session limit or the sender's share was reached. | Retry with back-off. |
| `too_large` | The frame is larger than `max_frame`. | Fix the caller. Do not retry. |
| `bad_frame` | The frame or request is not valid. | Fix the caller. Do not retry. |
| `unavailable` | The broker cannot serve now. | Retry with back-off. |
| `timeout` | A deadline passed (used by adapters for RPC deadlines). | Adapter decides. |
| `peer_gone` | The other side of a channel left. | Close the channel. |

A channel `close` frame carries `status` with `ok` or one of these codes.

## 9. Broker API and store interface

### 9.1 Data operations (node credential)

These are the operations a node can call on a broker, in language-neutral form. Section 10 gives them over HTTP.

| Operation | Input | Output | Notes |
|---|---|---|---|
| `connect` | connection string | a connection | Fails with `unauthorized` or `session_gone`. |
| `info` | none | `spec_version`, `max_frame`, `frame_ttl` | A caller refuses a broker whose `spec_version` it does not support. |
| `send` | `dest`, frame, optional `ttl` | success, or an error | `dest` is any live address of the session. |
| `recv` | `wait` seconds | a list of `(delivery_id, src, frame)` with at least one entry, or empty after `wait` | Long poll. Reads the caller's own address. The list holds up to `recv_batch_bytes` (*provisional*: 8 MiB) of frames, and always at least one, small-lane frames first (section 7.4). Many calls may run at once. A `delivery_id` is opaque: a node only passes it back to `ack`. |
| `ack` | list of `delivery_id` | success | Called on receipt (section 7.1). |
| `close` | none | none | Releases the connection. |

`connect` and `close` act on the client side only. They send no HTTP request, so they are not in section 10.

### 9.2 Control operations (launcher)

| Operation | Credential | Input | Output |
|---|---|---|---|
| `register` | master secret | none | `session_id`, admin credential |
| `mint` | admin | list of addresses | map of address to node credential |
| `revoke` | admin | none | none (idempotent) |
| `renew` | admin | none | seconds left on the lease and the session TTL |
| `alive` | any | none | seconds left, or `session_gone` |
| `presence` | admin | none | map of address to `{joined, idle}` |
| `stats` | admin | none | counters per session (frames and bytes per address, session bytes) |

**Presence.** A node is `joined` after its first successful `send` or `recv`. `idle` is the number of seconds since its latest `send`, `recv` or `ack`, or null before it has joined. A launcher that waits for all nodes to join waits for `joined`. A launcher that watches for a silent node reads `idle`. `joined` replaces the old Flower-specific `register-node` flag.

### 9.3 Store interface

The store is internal to a broker. It is not part of the wire protocol. A store is a set of named queues with a visibility timeout. It holds no credentials, applies no limits, knows nothing about lanes and never waits. The broker does all of that.

- **Names.** The broker names the queues. The small lane of an address is the address. Its bulk lane is the address followed by `#bulk`. No address contains `#`. To the store a queue is just a name.
- **Ledger.** The broker keeps a ledger in memory of every entry it has queued (lane, sender, size, expiry). The ledger makes the limits, the sender share and expiry exact whatever store sits behind the broker. If the broker restarts, the ledger is lost, and so is every session, so nothing that matters is lost.
- **Waiting.** A `recv` that finds nothing waits in the broker, not in the store. A `send` to that node wakes it. Otherwise it looks again every `recv_recheck` (*provisional*: 0.5 s), because a store redelivers silently when a visibility timeout ends. A store therefore needs no blocking read.

| Operation | Meaning |
|---|---|
| `create(session)` | A session exists. Called once, before any other call for it. |
| `push(session, queue, entry)` | Queue an entry (id, sender, frame, expiry time). |
| `pop(session, queue, max_bytes, visible_for)` | Return at once. Give `(delivery_id, entry)` pairs: up to `max_bytes` of frames, but at least one if any is queued. Give an empty list if none is. An entry that is not acked within `visible_for` seconds is queued again with a new `delivery_id`. An expired entry is never returned. |
| `ack(session, queue, delivery_ids)` | Remove delivered entries. Return the entry ids really removed. Unknown or old ids are ignored. |
| `purge(session)` | The session ended. Delete everything. |

A store needs no ordering guarantee and no per-sender counts, and it needs no priorities. It must honour an entry's expiry time on the clock it was given. The in-memory store is the default and the reference. This interface is **provisional** until a second store exists to test it.

## 10. The broker's HTTP API

The endpoints are defined in `docs/openapi.yaml`. A test checks that the file is valid and matches the real routes. This table is a summary.

| Operation | Request | Success | Credential |
|---|---|---|---|
| `register` | `POST /sessions`, header `X-Master-Secret` | `201`, JSON `session_id`, `credential` | master secret |
| `mint` | `POST /sessions/{id}/nodes`, JSON `{"addresses": [...]}` | `200`, JSON map address to credential | admin |
| `revoke` | `DELETE /sessions/{id}` | `204` | admin |
| `renew` | `POST /sessions/{id}/renew` | `200`, JSON remaining | admin |
| `alive` | `GET /sessions/{id}/alive` | `200`, JSON remaining | any |
| `presence` | `GET /sessions/{id}/presence` | `200`, JSON | admin |
| `stats` | `GET /sessions/{id}/stats` | `200`, JSON | admin |
| `info` | `GET /sessions/{id}/info` | `200`, JSON | node |
| `send` | `POST /sessions/{id}/send/{dest}?ttl=S`, body = frame (`application/octet-stream`) | `204` | node |
| `recv` | `GET /sessions/{id}/recv?wait=S` | `200`, body = records (below); `204` if empty | node |
| `ack` | `POST /sessions/{id}/ack`, JSON `{"ids": [...]}` | `204` | node |

All requests except `register` carry `Authorization: Bearer <credential>`. There is no wildcard DNS name and no Host-header parsing. The broker answers no `HEAD` requests, because a `HEAD` on `recv` would take a frame off the queue.

**`recv` body.** The body is a series of records, each: 4 bytes record length (big-endian), 8 bytes `delivery_id` (big-endian), 1 byte `src` length, the `src` bytes (UTF-8), then the frame bytes (the rest of the record).

**Reverse proxy.** `POST /sessions` is the only way to guess the master secret without a credential. The reverse proxy MUST limit it per source address (`nginx.conf.example` does this).

Error mapping:

| Code | HTTP status |
|---|---|
| `unauthorized` | 401 |
| `forbidden` | 403 |
| `session_gone` | 410 |
| `unknown_dest` | 404 |
| `conflict` | 409 |
| `queue_full` | 429 (with `Retry-After`) |
| `too_large` | 413 |
| `bad_frame` | 400 |
| `unavailable` | 503 |

An error reply has a JSON body `{"error": <code>, "detail": <text>}`. `recv` is a long poll started by the node. The broker never starts a connection.

## 11. Adapters

An adapter is not part of the broker API. These notes record what the first two adapters need from the layers below. Their design is a separate document. In both, the orchestrator tells each adapter its own address and its peers' addresses. The hub has the address `hub`, and the hub adapter replies to the `src` of each frame. Spoke adapters send to `hub` and accept only frames from `hub`. This is the adapters' convention, not the broker's.

### 11.1 Flower

- **Spoke adapter (beside each SuperNode):** a local HTTP server. The SuperNode connects to `127.0.0.1`. The adapter turns each request into an RPC channel and returns the response. `meta` carries the path and headers.
- **Hub adapter (beside the SuperLink):** it applies the allowlist of `/v1/fleet/*` paths and replays calls to the local SuperLink.
- **The spoke adapter must map errors to what the SuperNode accepts** (read from Flower 1.40.0, `flwr/supercore/retry/http_retry.py`):
  - The SuperNode retries forever, with back-off up to 20 s, on 503, 504 and on connect, read, write and pool errors.
  - `403` makes it stop cleanly (`RunNotRunningException`). `401` makes it stop the process.
  - Any other status, including `404`, `410`, `429` and `502`, makes it give up with an error.
  - So `session_gone` MUST become `403`. `queue_full`, `unavailable` and an RPC `timeout` MUST become `503` or `504`. A `429` from the broker MUST NOT reach the SuperNode. `unknown_dest` is a configuration fault and may become `503`.
- **Time budget:** the SuperNode's default call timeout is 30 s. The heartbeat call (`send-node-heartbeat`) has **5 s**. The broker round trip, plus the hub adapter, plus the SuperLink must answer a heartbeat inside 5 s. The RPC deadline of the adapter should be below 30 s.
- **Duplicates:** a SuperNode retry after a timeout is a new channel (section 7.3). Tested with Flower 1.43.0 and `quickstart_numpy` (2 SuperNodes, 5 rounds): when the answers to 6 `push-messages` calls were cut, the SuperNodes retried, the SuperLink ran the repeats, and the app finished (`docs/measurements.md`, section 1). One app only.

### 11.2 FLARE

*Design only: there is no FLARE adapter yet.*

- **Method:** a plain byte tunnel for `tcp` (or `http`). FLARE keeps its own mTLS from end to end through the adapters and the broker. The adapters do not end TLS. The broker carries ciphertext.
- **Spoke adapter (FLARE client site):** it listens on a fixed local address. FLARE connects to it. Each accepted connection opens one stream channel to `hub`.
- **Hub adapter (FLARE server):** for each opened channel it opens an outbound connection to the local FLARE server port and copies bytes both ways. The adapter never accepts a connection from the network.
- **What was read in NVFlare (commit 3a7f077):** `get_ssl_context` sets `check_hostname = False` and checks the certificate against the provisioned CA only, so a localhost target does not break the handshake for the tcp driver. The `tcp` driver makes one outbound socket and reads on the same connection. FLARE reconnects a dropped connector by itself, so closing a channel is recoverable.
- **Time budget:** FLARE sends a heartbeat PING after 60 s idle and does not close on a missing reply. A blocked send fails after 30 s (`streaming_send_timeout`) and a stall warning appears after 45 s. The adapter must read its local socket only while the broker accepts data, and a stalled send must not last 30 s.
- **Limit:** a byte tunnel cannot filter messages. FLARE's own authorization does that.
- **Not covered:** the `grpc` driver builds its own credentials. A localhost target may fail its host check. This is not tested. Use `tcp` or `http`.

## 12. Parameters (provisional)

Each one is a setting, and each default is provisional. `docs/measurements.md` (one machine, loopback, a modelled link) supports `ack_timeout`, the back-off, `recv_recheck`, the lanes and the poller count. The others are guesses. Nothing was measured on a real network or on Linux.

| Parameter | Default | To measure |
|---|---|---|
| `poll_wait` (long poll) | 25 s | Latency of the extra poll hop against the 5 s Flower heartbeat. |
| `ack_timeout` | 5 s | **Must exceed `p × B ÷ R`** (section 7.4). Too short collapses a slow link (`docs/measurements.md`, section 2). |
| frame TTL (`frame_ttl`) | 120 s | Longest queue wait that is still useful. Also sets how long closed channels are remembered (`2 ×`). |
| `sender_share` | 1/4 of the queue limit | Heartbeat latency during a simultaneous upload of model updates. |
| `coalesce_delay` | 10 ms | Throughput of the FLARE tunnel against added latency. |
| `recv_batch_bytes` | 8 MiB | Throughput and latency of small frames. |
| `lease` | 120 s | Lease renewal every 30 s. |
| session TTL (`session_ttl`) | 6 h | Longest real session. |
| `max_frame` | 4 MiB | Throughput and memory. FLARE chunks at 1 MB. |
| queue limit per address, bulk lane | 1024 frames or 64 MiB | Memory per session. |
| `small_frame_bytes` | 64 KiB | Largest control frame (a Flower heartbeat is far smaller). |
| queue limit per address, small lane | 1024 frames or 4 MiB | Memory, against the number of small frames a burst sends. |
| session total limit (bulk lanes) | 256 MiB | Memory per session. |
| `recv_recheck` | 0.5 s | Measured (`docs/measurements.md`, section 2): longer saves little CPU, shorter helps little. |
| `reorder_bytes` | 64 MiB per channel | Largest message that must survive a lost ack, against receiver memory. |
| RPC deadline (Flower adapter) | 25 s | Below 30 s, and the real SuperLink latency. |
| send back-off | 0.05 s doubling to 0.25 s | Measured (`docs/measurements.md`, section 1): a 5 s cap collapsed throughput on small queues. |
| `max_nodes` | 64 | Largest federation. |

## 13. Conformance tests

There are two conformance suites.

**API conformance** runs against a broker, both in process and over HTTP. It checks:

- the four rules of section 4.3 (a node cannot read another address, cannot forge `src`, cannot reach another session, cannot call an admin operation);
- the size limit, the queue limits, the sender share and `unknown_dest`;
- at-least-once with ack and redelivery, `ttl` expiry, a late ack of an expired frame, and competing consumers;
- a repeated `mint` of an address that has not joined, and `conflict` for one that has;
- `revoke`, lease and session TTL behaviour.

**Store conformance** runs against each store. It checks push, pop, ack, the visibility timeout with new delivery ids, expiry, waiting pops and purge.

The endpoint tests add faults (duplicated sends, lost send replies, lost receive responses) and check that every RPC runs exactly once and gets its own reply.

## 14. Open items and out of scope

Open, to settle before or during the first build:

1. **FLARE job-level timeouts.** The client-to-server job heartbeat and cell request timeouts (in `nvflare/private/`) were not read. A slow broker could trip them above the tcp driver.
2. **FLARE `grpc` driver** over a localhost target (section 11.2).
3. **Flower duplicate tolerance** is tested for one app only (section 11.1 and `docs/measurements.md`, section 1). Other apps are not tested.
4. **Rendezvous beyond an environment variable.** The orchestrator delivers connection strings and addresses. Other delivery methods are not specified.
5. **Stores.** The in-memory store loses everything on restart. External stores (for example NATS) are future work, and the store interface (section 9.3) is provisional until one exists. Several broker processes sharing one store would also need shared session state, which is not designed.
6. **Flooding.** A hostile node can use its whole share of every queue (section 7.4). Tighter limits are possible if nodes of one session are ever mutually untrusted parties.
7. **Launcher watchdog.** Which `presence` rules end a session is left to the launcher. A shared library function for the common cases is not specified.
8. **Stream mode.** Not yet implemented. Coalescing and interaction with a slow consumer are not measured.
9. **Heartbeats in mixed answers.** A small frame can still ride in a `recv` answer that also carries bulk frames, and waits for the whole answer (`docs/measurements.md`, section 2). A `recv` for the small lane alone, polled by dedicated pollers in the endpoint, would remove that. It changes the HTTP API (a `lane` parameter) and the `recv` operation (section 9.1). Not built.
10. **Hub pollers.** The Flower hub adapter receives with 4 pollers (`docs/measurements.md`, section 2) and allows 16 calls to the SuperLink at once. If the hub's link to the broker is slower than about 10 MB/s, or batches are larger, `ack_timeout` must grow with `p × B ÷ R` (section 7.4).

Out of scope for version 1:

- End-to-end encryption by the endpoint. FLARE gets it from mTLS. Flower does not get it yet.
- Send restrictions between nodes in the broker. Adapters drop frames from unexpected senders.
- Broadcast. An adapter sends to each address in turn.
- Nodes that relay for other nodes (FLARE hierarchies) need no broker change, but are not designed.
- Connecting to an application deployment that already exists outside the session. A session is created with fresh processes.
- Stores other than the in-memory default.
- Removing a single node, and capability flags such as `ordered` and `persistent`. A crashed node is replaced by minting a new address.
- A command-line interface. Orchestration is a library for now.
- Credit-based flow control and per-credential rate limits.
