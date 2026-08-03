import json
from abc import ABC, abstractmethod
from typing import Any, Dict, Type

from .messages import Message


class Serializer(ABC):
    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError

    @abstractmethod
    def encode(self, message: Message) -> bytes:
        raise NotImplementedError

    @abstractmethod
    def decode(self, buffer: bytes, schema_name: str) -> Message:
        raise NotImplementedError

    @abstractmethod
    def register(self, message_type: Type[Message]) -> None:
        raise NotImplementedError


class JsonSerializer(Serializer):
    """JSON serializer compatible with the go-rabbitbus JSON serializer."""

    def __init__(self):
        self._registry: Dict[str, Type[Message]] = {}

    def name(self) -> str:
        return "json"

    def register(self, message_type: Type[Message]) -> None:
        self._registry[message_type().schema_name()] = message_type

    def encode(self, message: Message) -> bytes:
        if not isinstance(message, Message):
            raise ValueError("message must implement Message interface")
        data = {
            "schema_name": message.schema_name(),
            "payload": message.__dict__,
        }
        return json.dumps(data, separators=(",", ":"), default=str).encode("utf-8")

    def decode(self, buffer: bytes, schema_name: str) -> Message:
        message_type = self._registry.get(schema_name)
        if message_type is None:
            raise ValueError(f"unregistered message schema: {schema_name}")
        data = json.loads(buffer.decode("utf-8"))
        payload_data = data.get("payload", {})
        return message_type(**payload_data)
