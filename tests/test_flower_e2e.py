"""End to end: a real broker process, a real Flower SuperLink and SuperNodes, a real Flower app (quickstart_numpy),
and our adapters between them. Skipped when flwr is not installed (pip install -e ".[e2e]")."""

import pytest

pytest.importorskip("flwr")

from flower_stack import FlowerStack  # noqa: E402


@pytest.mark.timeout(400)
async def test_flower_app_trains_through_the_broker(tmp_path):
    stack = FlowerStack(tmp_path)
    try:
        await stack.start()
        await stack.run_app()
        await stack.end_session()
        # the launcher ended the session: every adapter must notice and exit cleanly (spec 4.1, 11.1)
        assert await stack.adapters_exit_codes() == [0] * (1 + stack.n)
    finally:
        await stack.close()
