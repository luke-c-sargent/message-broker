import pytest

from message_broker import BrokerError
from message_broker.frames import Header, decode, encode


def test_round_trip_and_the_payload_is_a_view_not_a_copy():
    frame = encode(Header("c", 3, "data", more=True, meta={"k": "v"}), b"payload")
    h, payload = decode(frame)
    assert (h.ch, h.seq, h.kind, h.more, h.meta) == ("c", 3, "data", True, {"k": "v"})
    assert isinstance(payload, memoryview) and payload.obj is frame and bytes(payload) == b"payload"


def test_a_view_can_be_encoded_and_decoded_again():
    frame = encode(Header("c", 0, "open", mode="rpc"), memoryview(b"0123456789")[2:6])
    assert bytes(decode(memoryview(frame))[1]) == b"2345"


@pytest.mark.parametrize("bad", [b"", b"\x00\x00\x00\x09{not json", b"\x00\x00\x00\x02{}", b"\xff\xff\xff\xffxx"])
def test_a_bad_frame_is_refused(bad):
    with pytest.raises(BrokerError) as e:
        decode(bad)
    assert e.value.code == "bad_frame"
