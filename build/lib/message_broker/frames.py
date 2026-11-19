"""Frame encoding of spec section 5: [4-byte header length][JSON header][raw payload]."""

import json
import struct
from dataclasses import dataclass, field

from .errors import BrokerError

VERSION = 1
KINDS = {"open", "data", "close"}


@dataclass
class Header:
    ch: str
    seq: int
    kind: str
    mode: str | None = None  # on open: "rpc" | "stream"
    more: bool = False  # the message continues in the next frame
    rsp: bool = False  # sent by the responder (the side that did not open the channel)
    status: str | None = None  # on close
    meta: dict[str, str] = field(default_factory=dict)


def encode(h: Header, payload: bytes | bytearray | memoryview = b"") -> bytes:
    d = {"v": VERSION, "ch": h.ch, "seq": h.seq, "kind": h.kind}
    if h.mode:
        d["mode"] = h.mode
    if h.more:
        d["more"] = True
    if h.rsp:
        d["rsp"] = True
    if h.status:
        d["status"] = h.status
    if h.meta:
        d["meta"] = h.meta
    raw = json.dumps(d, separators=(",", ":")).encode()
    return b"".join((struct.pack(">I", len(raw)), raw, payload))  # the one copy of a payload on the way out


def decode(frame: bytes | memoryview) -> tuple[Header, memoryview]:
    """The payload comes back as a view into `frame`, not a copy."""
    try:
        view = memoryview(frame)
        (n,) = struct.unpack_from(">I", view)
        d = json.loads(bytes(view[4:4 + n]))  # only the small header is copied
        if d["v"] != VERSION or d["kind"] not in KINDS or not isinstance(d["seq"], int) or d["seq"] < 0:
            raise ValueError("unsupported header")
        h = Header(str(d["ch"]), d["seq"], d["kind"], d.get("mode"), bool(d.get("more")),
                   bool(d.get("rsp")), d.get("status"), dict(d.get("meta") or {}))
        if h.kind == "open" and h.mode not in ("rpc", "stream"):
            raise ValueError("open needs a mode")
        return h, view[4 + n:]
    except (struct.error, ValueError, KeyError, TypeError) as e:  # json errors are ValueErrors
        raise BrokerError("bad_frame", str(e)) from e
