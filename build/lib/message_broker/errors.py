"""Error codes of spec section 8. The broker raises BrokerError; the HTTP layer maps code -> status."""

CODES = {"unauthorized", "forbidden", "session_gone", "unknown_dest", "conflict", "queue_full",
         "too_large", "bad_frame", "unavailable", "timeout", "peer_gone"}
RETRYABLE = {"queue_full", "unavailable"}  # the caller waits and sends the same frame again


class BrokerError(Exception):
    def __init__(self, code: str, detail: str = ""):
        assert code in CODES, code
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code, self.detail = code, detail
