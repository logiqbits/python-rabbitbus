from typing import Optional

import pika

from .messages import BusMessage
from .policies import MessagePolicy


class Invocation:
    """Context passed to message handlers."""

    def __init__(
        self,
        bus: "Bus",  # noqa: F821
        inbound_message: BusMessage,
        reply_to: Optional[str],
        exchange: str,
        routing_key: str,
    ):
        self._bus = bus
        self._inbound_message = inbound_message
        self._reply_to = reply_to
        self._exchange = exchange
        self._routing_key = routing_key

    def reply(self, message: BusMessage) -> None:
        if self._inbound_message:
            message.correlation_id = self._inbound_message.id
            message.saga_correlation_id = self._inbound_message.saga_id
            message.rpc_id = self._inbound_message.rpc_id
        if self._reply_to:
            self._bus._send_raw(
                exchange="",
                routing_key=self._reply_to,
                message=message,
            )

    def send(self, to_service: str, message: BusMessage) -> None:
        self._bus.send(to_service, message)

    def publish(self, exchange: str, topic: str, message: BusMessage) -> None:
        self._bus.publish(exchange, topic, message)

    def routing(self) -> tuple:
        return self._exchange, self._routing_key
