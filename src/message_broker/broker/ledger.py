"""The ledger of one session: every entry the broker has queued (lane, sender, size, expiry), and the limits
that follow from it (spec 7.4, 9.3). It decides whether a frame may be queued. It does not touch the store, so
the limits and expiry are exact whatever store sits behind the broker."""

import heapq
from dataclasses import dataclass, field

SMALL, BULK = 0, 1  # the two lanes of an address (spec 7.4)


@dataclass
class Lane:
    """What one lane of one address holds: queued and delivered-but-unacked frames."""
    frames: int = 0
    bytes: int = 0
    by_sender: dict = field(default_factory=dict)  # src -> [frames, bytes]


class Ledger:
    def __init__(self, lim):  # lim: the broker's Limits
        self.lim = lim
        self.bulk_bytes = 0  # all bulk lanes of the session together
        self._entries = {}  # entry id -> (lane, kind, src, size)
        self._expiry = []  # heap of (expires, entry id)

    def admit(self, eid: int, lane: Lane, kind: int, src: str, size: int, expires: float) -> bool:
        """Count a frame for `lane` (of kind SMALL or BULK) if every limit allows it. False if one would be passed."""
        lim = self.lim
        cap_bytes, floor = ((lim.small_queue_bytes, lim.small_frame_bytes) if kind == SMALL
                            else (lim.queue_bytes, lim.max_frame))
        sent = lane.by_sender.setdefault(src, [0, 0])
        if (lane.frames + 1 > lim.queue_frames or lane.bytes + size > cap_bytes
                or (kind == BULK and self.bulk_bytes + size > lim.session_bytes)
                or sent[0] + 1 > max(1, int(lim.queue_frames * lim.sender_share))
                or sent[1] + size > max(floor, int(cap_bytes * lim.sender_share))):
            return False
        self._entries[eid] = (lane, kind, src, size)
        heapq.heappush(self._expiry, (expires, eid))
        lane.frames += 1
        lane.bytes += size
        sent[0] += 1
        sent[1] += size
        if kind == BULK:
            self.bulk_bytes += size
        return True

    def release(self, eid: int) -> None:
        meta = self._entries.pop(eid, None)  # None: already released (acked or expired)
        if meta:
            lane, kind, src, size = meta
            lane.frames -= 1
            lane.bytes -= size
            lane.by_sender[src][0] -= 1
            lane.by_sender[src][1] -= size
            if kind == BULK:
                self.bulk_bytes -= size

    def expire(self, now: float) -> None:
        """Release entries whose ttl ended, so limits count only frames that can still be delivered."""
        while self._expiry and self._expiry[0][0] <= now:
            self.release(heapq.heappop(self._expiry)[1])
