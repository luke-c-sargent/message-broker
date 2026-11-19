# Measurements

Evidence for the provisional parameters in spec section 12. Not part of the specification.

Everything here was measured on one machine (Apple M3 Max, 16 cores, 64 GB, macOS, Python 3.12), over loopback or a modelled link. Sections 1 and 2 were not measured on a real network or on Linux; section 3 was. The load generator is `bench/load.py`.

## 1. Bursts, memory and faults

In this file a "heartbeat" is the application heartbeat: a small RPC every second that stands in for Flower's `send-node-heartbeat`. Taken on one machine (Apple M3 Max, 16 cores, 64 GB, macOS, Python 3.12). The broker ran as its own process on loopback. The load generator (`bench/load.py`) ran all nodes in a single process, so it is a harder case for CPU than real nodes on separate hosts. Its event-loop lag was 100 ms or less. The hub answered at once, so the SuperLink's own time is **not** included. Treat them as numbers for the broker and the endpoint, not for a deployment.

**Heartbeats under a burst.** 32 spokes each send a small RPC every second. At the same time each uploads a large message. Flower gives a heartbeat 5 s.

| Hub's link to the broker | Queue per address | Backoff cap after `queue_full` | Heartbeat p50 | p99 | Beats over 5 s | Upload rate |
|---|---|---|---|---|---|---|
| unlimited (loopback) | 64 MiB | 5 s | 22 ms | 199 ms | 0 | 333 MB/s |
| 60 MB/s | 64 MiB | 5 s | 846 ms | 1.15 s | 0 | 49 MB/s |
| 60 MB/s, no sender share | 64 MiB | 5 s | 806 ms | 1.78 s (max) | 0 | 43 MB/s |
| 10 MB/s | 64 MiB | 5 s | **4981 ms** | 6.2 s | **64 of 160** | 7 MB/s |
| 10 MB/s | 16 MiB | 5 s | 14 ms | 1.6 s | 0 | 5 MB/s |
| 10 MB/s | 16 MiB | 0.25 s | 1.28 s | 2.2 s | 0 | 10 MB/s |
| 10 MB/s | 8 MiB | 5 s | 14 ms | 0.9 s | 0 | **2 MB/s** |
| 10 MB/s | 8 MiB | 0.25 s | 571 ms | 1.3 s | 0 | 9 MB/s |

(The 10 MB/s rows move 5 MB per spoke, the others 20 MB. Rows vary a little between runs.)

**Memory.** 32 spokes moving 20 MB each (640 MB, against a session limit of 256 MiB):

- The broker process reached a peak of **1.1 to 1.8 GB** and did not give it back when idle. That is 4 to 7 times `session_bytes`. A smaller `recv_batch_bytes` (1 MiB) did not change it.
- It is not a leak. In a run with tracemalloc, the memory still referenced after each burst stayed flat (74 MiB over four bursts), and the ledger was empty. The rest is the allocator keeping its high-water mark. At the heaviest moment of a burst (tracemalloc, broker and nodes in one process, 1.37 GB live) the broker's own code and aiohttp's server side held about 215 MiB, close to the limits. The node-side code (endpoint and HTTP client) held most of the rest: the whole message copied into a list of chunks before sending (620 MiB, one copy of every message being sent), frames sliced out of their headers (204 MiB and 92 MiB), encoded frames (124 MiB). The broker's own RSS of 1.1 to 1.8 GB is therefore mostly memory the allocator did not return, not live data. This was measured on macOS. Linux may behave differently.
- `session_bytes` therefore does **not** bound process memory. Whoever runs the broker needs several times that amount of RAM per busy session.

**Throughput.** 330 to 430 MB/s through the broker on loopback with 4 MiB frames (before the copy changes below). 390 to 500 MB/s after.

**Faults with real Flower.** Flower 1.43.0, `quickstart_numpy`, 2 SuperNodes, 5 rounds. The traffic between the adapters and the broker had 5 % chances of: a duplicated `send`, a lost `send` answer, a lost `recv` answer, a lost `ack` (about 150 faults in one run, seeded). The app finished. SuperNodes sent 310 Fleet requests. The SuperLink ran 310. The adapters caused no execution beyond what the SuperNodes sent. 38 requests were sent more than once by the SuperNodes themselves, nearly all `pull-object` reads.

**What the numbers mean**

1. **Heartbeat delay is set by queue depth, not by the sender share.** A heartbeat waits behind every frame ahead of it in the queue. With a slow link from the hub to the broker it waits about `queue_bytes ÷ link rate`. At 10 MB/s and 64 MiB that is over 6 s, past Flower's 5 s. The sender share only trims the tail.
2. **The 5 s backoff cap collapses throughput on a small queue.** After `queue_full` a sender may sleep up to 5 s while room frees up much sooner. With a 0.25 s cap the same small queue ran at the full link rate.
3. **A small queue plus a short backoff works.** A queue of about one second of the hub's link rate (8 MiB at 10 MB/s) and a cap of 0.25 s gave p99 heartbeats of 1.3 s at the full link rate.

**Applied since:** the back-off cap is now 0.25 s, the broker has a small lane and a bulk lane (spec section 7.4), and the node-side copies of a message were cut (below). The slow-link rows above were measured **before the lanes**, and with a link model that carried one answer at a time. Section 2 repeats the slow-link test with a link shared by concurrent answers, as TCP does.

**Memory after cutting the copies.** The node-side code now slices chunks as views, takes payloads and received frames as views, rebuilds a received message in one growing buffer, and the broker streams its `recv` answer. The same burst (32 spokes, 20 MB each):

| | Before | After |
|---|---|---|
| Live memory at the heaviest moment (broker and nodes in one process) | 1367 MiB | 613 MiB |
| of it: node-side code | 713 MiB | 155 MiB |
| of it: broker's own code | 72 MiB | 0 MiB |
| Broker process RSS after three bursts | 1080, 1536, 1756 MB | 638, 1240, 1310 MB |
| Upload / download rate | 333 / 428 MB/s | 394 / 495 MB/s |

What is left is mostly aiohttp's own buffers (312 MiB) and the message being rebuilt at the receiver (155 MiB, which is the message itself). A node's peak for one message is now about the size of that message plus one frame, and a test checks that (under 1.6 times). The broker's RSS still rises with each burst. That is the allocator keeping memory, and Linux may differ.

## 2. Lanes, the re-check and slow links

Same machine and harness as section 1. The hub's link is modelled as `R` MB/s shared fairly by the answers in transit, and an answer reaches the hub only when it has fully arrived. 32 spokes each upload 5 MB and send a heartbeat every second.

| Pollers of the hub's endpoint `p` | `recv_batch_bytes` | `ack_timeout` | Heartbeat p50 | p99 | Result at 10 MB/s |
|---|---|---|---|---|---|
| 16 | 8 MiB | 5 s (default) | 10.3 s | 10.9 s | **collapse.** 13 of 32 uploads failed; in a separate run all 32 failed at the 120 s deadline |
| 4 | 8 MiB | 5 s | 2.2 s | 3.6 s | all done, 10 MB/s |
| 4 | 4 MiB | 5 s | 1.95 s | 2.3 s | all done, 10 MB/s |
| 2 | 4 MiB | 5 s | 1.0 s | 1.75 s | all done, 9 MB/s |
| 16 | 8 MiB | 30 s | **33 ms** | 5.5 s | all done, 10 MB/s |
| 16 | 4 MiB | 30 s | 166 ms | 7.1 s | all done, 10 MB/s |

What this shows:

1. **The collapse is redelivery outrunning delivery.** 16 pollers each hold up to 8 MiB, which is 128 MiB in flight. At 10 MB/s that takes 13 s to arrive, longer than the 5 s `ack_timeout`. The broker redelivers frames still arriving, and nothing completes. The rule in section 7.4 (`ack_timeout > p × B ÷ R`) predicts it.
2. **The lanes work.** With a long enough `ack_timeout` the median heartbeat is 33 ms on a congested link, where it was 5 s before the lanes (section 1).
3. **The tail is heartbeats that ride in a mixed answer.** A heartbeat popped in the same batch as bulk frames arrives only when the whole answer has. With 16 pollers that is several seconds. Fewer pollers and a smaller batch shrink it (p99 1.75 s at `p` = 2, batch 4 MiB). A `recv` for the small lane alone would remove it. That is not built (spec section 14, limitation 1).
4. **Fewer pollers on the hub cost nothing on a fast link.** On loopback with no throttle (32 spokes, 20 MB each): upload 401 MB/s and download 490 MB/s with 16 pollers, 374 and 511 with 4. Heartbeat p99 stayed under 250 ms either way. Before the lanes it was 390 to 500 MB/s.

**The re-check.** A waiting `recv` looks again every `recv_recheck` because a store redelivers silently. Test: 32 spokes, 5 % of the hub's `recv` answers lost, `ack_timeout` 1 s, a heartbeat every second.

| `recv_recheck` | Heartbeat p99 (hub with 16 pollers) | Broker CPU | Heartbeat p99 (hub with 1 poller) | Broker CPU |
|---|---|---|---|---|
| 0.05 s | 1065 ms | 11 % | | |
| 0.1 s | | | 1100 ms | 8 % |
| 0.25 s | 1039 ms | 5 % | | |
| 0.5 s | 1026 ms | 4 % | 1014 ms | 3 % |
| 1 s | 1035 ms | 3 % | | |
| 2 s | 1025 ms | 3 % | 2030 ms | 2 % |

The p99 of about 1 s is the `ack_timeout` itself: a lost answer costs one visibility timeout. The re-check adds little because the timers of 16 pollers are staggered, so together they look 16 times as often. A lone poller waits up to a whole re-check on top (2.0 s at a 2 s re-check). At 0.5 s that is hidden by the timeout. Shorter than 0.25 s costs CPU for no gain. Longer than 1 s only matters to a node with one poller. **0.5 s stays the default.**

## 3. A live server on Linux

Evidence from a deployed broker, with nginx and TLS in front, as in `nginx.conf.example`. Taken on 2026-11-19. The server: 2 vCPU, 15 GB, Ubuntu with Python 3.12.3 and aiohttp 3.14.5, one broker process with the default limits. The deployed code was identical to the working tree. The load generator (`bench/load.py --url`) ran in one process, either on the server itself (no WAN) or on a laptop (a WAN link of 7 to 9 MB/s). The generator shared the 2 vCPUs with nginx and the broker when it ran on the server, so the throughput figures are low for this hardware. "Broker CPU" is a share of one core. "Heartbeat" is as in section 1.

**Bursts on the server.** Each spoke uploads, then downloads, a message of the stated size. Heartbeats continue throughout.

| Path | Spokes x size | Upload rate | Upload heartbeat p50 / p99 | Broker CPU | Download rate | Download heartbeat p50 / p99 | Broker RSS peak |
|---|---|---|---|---|---|---|---|
| direct to the broker (no nginx, no TLS) | 32 x 20 MB | 211 MB/s | 145 ms / 408 ms | 99 % | 276 MB/s | 291 ms / 708 ms | 541 MB |
| through nginx and TLS | 32 x 20 MB | 73 to 74 MB/s | 728 to 776 ms / 1.3 to 2.3 s | 46 % | 99 MB/s | 1.1 s / 3.4 s | 555 MB |
| through nginx and TLS | 63 x 10 MB | 54 MB/s | 1.05 s / 2.7 s | 43 % | 96 MB/s | 1.8 s / 4.5 s | 509 MB |

No transfer failed and no heartbeat went over 5 s in these runs. With no load, heartbeat p50 was 21 ms direct and 24 ms through nginx. In the 63-spoke run the generator's event-loop lag reached 694 ms, so the generator is part of what was measured.

**Memory.** Six upload bursts in a row (32 x 20 MB, through nginx): the broker's RSS was 470 MB after every one. It did not grow. The peak across all runs was 555 MB, against 1.1 to 1.8 GB for the same burst on macOS (section 1). `session_bytes` is 256 MiB, so on Linux the peak was about twice that.

**A lost answer.** 32 x 20 MB through nginx, with 5 % of the hub's `recv` answers lost (24 in the run) and the default `ack_timeout` of 5 s. Every transfer finished. Heartbeats did not keep to 5 s:

| Phase | Heartbeat p99 | Heartbeats over 5 s |
|---|---|---|
| no load | 5.6 s | 32 of 152 |
| upload | 5.1 s | 8 of 347 |
| download | 5.5 s | 5 of 141 |

This is the cost found in section 2 ("a lost answer costs one visibility timeout"), now at the default of 5 s instead of 1 s. A heartbeat whose answer was lost comes back after `ack_timeout` plus up to one `recv_recheck`, which is more than the 5 s Flower allows.

**A slow link on the client side.** 32 spokes and the hub on a laptop, 3 MB per transfer, the broker over the WAN:

| Phase | Rate | Heartbeat p50 / p99 | Heartbeats over 5 s |
|---|---|---|---|
| no load | | 284 ms / 860 ms | 0 |
| upload | 7 MB/s | 682 ms / 7.1 s | 32 of 192 |
| download | 9 MB/s | 732 ms / 1.6 s | 0 |

With 20 MB per transfer on the same link, every upload ran out of its 120 s deadline. The link, not the broker, was the limit: the broker used 8 to 12 % of a core and under 200 MB. The p99 of 7 s is the hub's link, as in section 2, item 3.

**Other findings.**

1. One broker process uses a whole core at about 210 to 280 MB/s (the direct runs). Through nginx and TLS on 2 vCPUs the limit was 54 to 99 MB/s.
2. A session holds at most 64 nodes. A 129-address `nodes` call (a hub and 128 spokes) gave `bad_frame: at most 64 nodes`.
3. After a restart the old credentials get `session_gone` (410) at once. A new session registers in under a second.
4. An unrenewed session was gone 136 s after it started (the lease is 120 s). A renewed one stayed.
5. nginx: a 4 MiB frame passes intact. A 9 MiB body gets nginx's own 413 (`client_max_body_size 8m`). A long poll with `wait=100` returned 204 after 30.1 s. `POST /sessions` was limited from the sixth request in a burst (429); frame traffic was not limited.

Not measured: the SuperLink's own time, real Flower on separate machines, other stores, and more than one session at once.
