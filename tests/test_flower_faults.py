"""A real Flower app through a broker whose traffic is damaged: sends arrive twice, answers get lost, acks get lost.
Checks the claims of spec sections 6.3 and 7.1 against real Flower: the app still trains, and the adapters never
make the SuperLink run a request more often than the SuperNodes sent it."""

from collections import Counter

import pytest

pytest.importorskip("flwr")

from flower_stack import FlowerStack  # noqa: E402

FAULTS = dict(seed=7, dup_send=0.05, lose_send=0.05, lose_recv=0.05, lose_ack=0.05)


@pytest.mark.timeout(900)
async def test_flower_survives_lost_and_duplicated_traffic(tmp_path):
    stack = FlowerStack(tmp_path, faults=FAULTS, broker_env={"BROKER_ACK_TIMEOUT": "1"})
    try:
        await stack.start()
        await stack.run_app()
        fleet = lambda seen: [k for k in seen if k[0].startswith("/v1/fleet/")]  # noqa: E731 (not the adapter's health check)
        sent = Counter(fleet(k for rec in stack.node_recs for k in rec.seen))  # what the SuperNodes sent, retries included
        reached = Counter(fleet(stack.link_rec.seen))  # what the SuperLink was asked to run
        extra = {k: reached[k] - sent[k] for k in reached if reached[k] > sent[k]}
        retried = {k for k, n in sent.items() if n > 1}  # the SuperNode itself sent these again (a new channel each time)
        ran_twice = Counter(k[0] for k in retried if reached[k] > 1)
        print(f"\nfaults injected: {stack.fault.count}"
              f"\nSuperNode requests: {sum(sent.values())} ({len(sent)} distinct, {len(retried)} sent more than once by the SuperNode)"
              f"\nSuperLink requests: {sum(reached.values())}; run more often than the SuperNodes sent them: {len(extra)}"
              f"\nrequests the SuperLink ran more than once because the SuperNode retried, by call: {dict(ran_twice)}")
        assert all(n > 0 for n in stack.fault.count.values()), stack.fault.count  # the faults really happened
        assert not extra, extra
    finally:
        await stack.close()


@pytest.mark.timeout(600)
async def test_flower_survives_a_repeated_push_messages(tmp_path):
    """The answer to the first 3 push-messages of each SuperNode is cut after the SuperLink ran them, so the
    SuperNode retries and the SuperLink gets the same push-messages again (spec 7.3: an application retry is a
    new channel, so the broker cannot remove it). Does Flower cope?"""
    stack = FlowerStack(tmp_path, node_lose_first={"/v1/fleet/push-messages": 3})
    try:
        await stack.start()
        await stack.run_app()
        cut = sum(3 - r.lose["/v1/fleet/push-messages"] for r in stack.node_recs)
        by_node = sum(1 for rec in stack.node_recs for k in rec.seen if k[0] == "/v1/fleet/push-messages")
        by_link = sum(1 for k in stack.link_rec.seen if k[0] == "/v1/fleet/push-messages")
        print(f"\npush-messages: {cut} answers cut, {by_node} sent by the SuperNodes, {by_link} run by the SuperLink")
        assert cut == 3 * stack.n  # every cut happened, so the SuperNodes had to retry; the app still finished
        assert by_link <= by_node  # and the adapters added none of their own
    finally:
        await stack.close()
