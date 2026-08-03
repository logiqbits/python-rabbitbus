import uuid
from typing import Any, Dict, Optional


class Message:
    """Base interface for all bus messages."""

    def schema_name(self) -> str:
        raise NotImplementedError


class BusMessage:
    """Envelope that is sent over the wire."""

    def __init__(
        self,
        payload: Message,
        message_id: Optional[str] = None,
        correlation_id: Optional[str] = None,
        saga_id: Optional[str] = None,
        saga_correlation_id: Optional[str] = None,
        rpc_id: Optional[str] = None,
    ):
        self.id = message_id or self._new_id()
        self.correlation_id = correlation_id
        self.saga_id = saga_id
        self.saga_correlation_id = saga_correlation_id
        self.rpc_id = rpc_id
        self.semantics: Optional[str] = None  # "cmd" or "evt"
        self.payload = payload
        self.payload_fqn = payload.schema_name()

    @staticmethod
    def _new_id() -> str:
        return uuid.uuid4().hex

    def to_amqp_headers(self) -> Dict[str, Any]:
        return {
            "x-msg-name": self.payload_fqn,
            "x-msg-saga-id": self.saga_id or "",
            "x-msg-saga-correlation-id": self.saga_correlation_id or "",
            "x-logiqbits-rabbitbus-msg-rpc-id": self.rpc_id or "",
        }

    @classmethod
    def from_amqp_headers(cls, headers: Dict[str, Any]) -> "BusMessage":
        def _get(key: str) -> Optional[str]:
            val = headers.get(key)
            return val if val else None

        bm = cls.__new__(cls)
        bm.saga_id = _get("x-msg-saga-id")
        bm.saga_correlation_id = _get("x-msg-saga-correlation-id")
        bm.rpc_id = _get("x-logiqbits-rabbitbus-msg-rpc-id")
        bm.payload_fqn = _get("x-msg-name") or ""
        bm.id = None
        bm.correlation_id = None
        bm.semantics = None
        bm.payload = None
        return bm
