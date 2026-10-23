from typing import List, Optional
from urllib.parse import urlparse, urlunparse

from .bus import DefaultBus
from .policies import MessagePolicy
from .serialization import JsonSerializer, Serializer


def _amqps(url: str) -> str:
    parts = urlparse(url)
    if parts.scheme == "amqps":
        return url
    return urlunparse(parts._replace(scheme="amqps"))


class Builder:
    def __init__(self):
        self._amqp_url: Optional[str] = None
        self._serializer: Optional[Serializer] = None
        self._default_policies: List[MessagePolicy] = []
        self._worker_num: int = 1
        self._prefetch_count: int = 1
        self._purge_on_startup: bool = False
        self._dlx: Optional[str] = None
        self._heartbeat: Optional[int] = 30
        self._blocked_timeout: Optional[float] = 300.0
        self._ca_certs: Optional[str] = None
        self._certfile: Optional[str] = None
        self._keyfile: Optional[str] = None
        self._server_hostname: Optional[str] = None

    def bus(self, amqp_url: str) -> "Builder":
        self._amqp_url = _amqps(amqp_url) if self._tls_enabled() else amqp_url
        return self

    def _tls_enabled(self) -> bool:
        return bool(
            self._ca_certs or self._certfile or self._keyfile or self._server_hostname
        )

    def heartbeat(self, seconds: Optional[int]) -> "Builder":
        """Heartbeat interval for both connections; None keeps the URL value."""
        self._heartbeat = seconds
        return self

    def blocked_timeout(self, seconds: Optional[float]) -> "Builder":
        """Blocked-connection timeout for both connections; None disables."""
        self._blocked_timeout = seconds
        return self

    def tls(
        self,
        ca_certs: Optional[str] = None,
        certfile: Optional[str] = None,
        keyfile: Optional[str] = None,
        server_hostname: Optional[str] = None,
    ) -> "Builder":
        """Enable TLS with optional custom CA and client certificate.

        Rewrites the URL scheme to amqps so a plain amqp:// URL cannot
        silently downgrade the connection.
        """
        self._ca_certs = ca_certs
        self._certfile = certfile
        self._keyfile = keyfile
        self._server_hostname = server_hostname
        if self._amqp_url:
            self._amqp_url = _amqps(self._amqp_url)
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
            heartbeat=self._heartbeat,
            blocked_timeout=self._blocked_timeout,
            ca_certs=self._ca_certs,
            certfile=self._certfile,
            keyfile=self._keyfile,
            server_hostname=self._server_hostname,
        )


def new() -> Builder:
    return Builder()
