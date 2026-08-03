from typing import List, Optional

from .bus import DefaultBus
from .policies import MessagePolicy
from .serialization import JsonSerializer, Serializer


class Builder:
    def __init__(self):
        self._amqp_url: Optional[str] = None
        self._serializer: Optional[Serializer] = None
        self._default_policies: List[MessagePolicy] = []
        self._worker_num: int = 1
        self._prefetch_count: int = 1
        self._purge_on_startup: bool = False
        self._dlx: Optional[str] = None

    def bus(self, amqp_url: str) -> "Builder":
        self._amqp_url = amqp_url
        return self

    def with_policies(self, *policies: MessagePolicy) -> "Builder":
        self._default_policies.extend(policies)
        return self

    def with_serializer(self, serializer: Serializer) -> "Builder":
        self._serializer = serializer
        return self

    def worker_num(self, workers: int, prefetch_count: int = 1) -> "Builder":
        self._worker_num = workers
        self._prefetch_count = prefetch_count
        return self

    def purge_on_startup(self) -> "Builder":
        self._purge_on_startup = True
        return self

    def with_deadlettering(self, deadletter_exchange: str) -> "Builder":
        self._dlx = deadletter_exchange
        return self

    def build(self, svc_name: str) -> DefaultBus:
        if not self._amqp_url:
            raise ValueError("amqp_url is required")
        return DefaultBus(
            amqp_url=self._amqp_url,
            svc_name=svc_name,
            serializer=self._serializer or JsonSerializer(),
            default_policies=self._default_policies,
            worker_num=self._worker_num,
            prefetch_count=self._prefetch_count,
            purge_on_startup=self._purge_on_startup,
            dlx=self._dlx,
        )


def new() -> Builder:
    return Builder()
