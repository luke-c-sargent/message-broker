"""The ledger alone: limits, sender share and expiry, with no broker or store."""

from message_broker import Limits
from message_broker.broker.ledger import BULK, SMALL, Lane, Ledger

LIM = Limits(queue_frames=4, small_queue_bytes=100, small_frame_bytes=10, queue_bytes=1000, max_frame=100,
             session_bytes=1500, sender_share=0.5)


def test_frame_and_byte_limits_per_lane():
    led, lane = Ledger(LIM), Lane()
    assert led.admit(1, lane, SMALL, "a", 10, 9) and led.admit(2, lane, SMALL, "b", 10, 9)
    assert (lane.frames, lane.bytes) == (2, 20)
    led.admit(3, lane, SMALL, "c", 10, 9), led.admit(4, lane, SMALL, "d", 10, 9)
    assert not led.admit(5, lane, SMALL, "e", 10, 9)  # queue_frames = 4


def test_a_sender_may_use_only_its_share_but_always_one_frame():
    led, lane = Ledger(LIM), Lane()
    assert led.admit(1, lane, BULK, "a", 100, 9) and led.admit(2, lane, BULK, "a", 100, 9)
    assert not led.admit(3, lane, BULK, "a", 100, 9)  # half of 4 frames
    assert led.admit(4, lane, BULK, "b", 100, 9)  # another sender still has room


def test_session_limit_counts_bulk_lanes_only():
    led = Ledger(LIM)
    lanes = [Lane(), Lane(), Lane()]
    for i, lane in enumerate(lanes):
        assert led.admit(i, lane, BULK, "a", 500, 9)
    assert led.bulk_bytes == 1500
    assert not led.admit(9, Lane(), BULK, "b", 1, 9)  # session_bytes
    assert led.admit(10, Lane(), SMALL, "b", 1, 9)  # the small lane is not part of it


def test_release_and_expire_free_the_room_once():
    led, lane = Ledger(LIM), Lane()
    led.admit(1, lane, BULK, "a", 100, 5.0)
    led.admit(2, lane, BULK, "a", 100, 50.0)
    led.release(1)
    led.release(1)  # a second release (an ack of an expired frame) changes nothing
    led.expire(10.0)  # 1 is already gone; 2 is still live
    assert (lane.frames, led.bulk_bytes) == (1, 100)
    led.expire(60.0)
    assert (lane.frames, lane.bytes, led.bulk_bytes) == (0, 0, 0)
