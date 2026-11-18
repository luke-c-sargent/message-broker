"""Small types both the broker and the client library use."""

from dataclasses import dataclass

SPEC_VERSION = "0.5"
KiB = 1024  # bytes
MiB = 1024 * KiB
SECONDS = 1.0  # durations are floats of seconds: write `25 * SECONDS`, `2 * MINUTES`
MINUTES = 60 * SECONDS
HOURS = 60 * MINUTES


@dataclass
class Info:
    spec_version: str
    max_frame: int
    frame_ttl: float


@dataclass
class Delivery:
    delivery_id: int
    src: str  # stamped by the broker; never taken from the frame
    frame: bytes | memoryview  # a view into the receive buffer when it came over HTTP
