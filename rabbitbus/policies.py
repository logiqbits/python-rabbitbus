from typing import Any, Dict

import pika


class MessagePolicy:
    def apply(self, properties: pika.BasicProperties) -> None:
        raise NotImplementedError


class Durable(MessagePolicy):
    def apply(self, properties: pika.BasicProperties) -> None:
        properties.delivery_mode = 2  # persistent


class NonDurable(MessagePolicy):
    def apply(self, properties: pika.BasicProperties) -> None:
        properties.delivery_mode = 1  # transient
