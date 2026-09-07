import logging
import threading
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Type

import pika
from pika.adapters.blocking_connection import BlockingChannel

from .invocation import Invocation
from .messages import BusMessage, Message
from .policies import MessagePolicy
from .serialization import JsonSerializer, Serializer

logger = logging.getLogger(__name__)

MessageHandler = Callable[[Invocation, BusMessage], Any]


class PublishError(Exception):
    """A message was not confirmed as delivered by the broker.

    Raised instead of silently dropping when publisher confirms fail
    (nack), the message is unroutable (mandatory publish returned), or
    the publish keeps failing after one reconnect+retry.
    """


def _wildcard_match(input_text: str, pattern: str) -> bool:
    in_parts = input_text.lower().split(".")
    pat_parts = pattern.lower().split(".")

    def _match_words(inp: List[str], pat: List[str]) -> bool:
        if not inp and not pat:
            return True
        if len(pat) > 1 and pat[0] == "*" and not inp:
            return False
        if (pat and pat[0] == "?") or (pat and inp and pat[0] == inp[0]):
            return _match_words(inp[1:], pat[1:])
        if pat and pat[0] == "*":
            return _match_words(inp, pat[1:]) or _match_words(inp[1:], pat)
        return False

    return _match_words(in_parts, pat_parts)


class _Registration:
    def __init__(
        self,
        exchange: str,
        routing_key: str,
        schema_name: Optional[str],
        handler: MessageHandler,
    ):
        self.exchange = exchange.lower()
        self.routing_key = routing_key.lower()
        self.schema_name = (schema_name or "").lower()
        self.handler = handler

    def matches(self, exchange: str, routing_key: str, msg_name: str) -> bool:
        target_exchange = exchange.lower()
        target_routing_key = routing_key.lower()
        target_msg_name = msg_name.lower()

        # command: empty exchange, exact routing key and message name match
        if self.exchange == "" and target_exchange == "":
            return (
                self.routing_key == target_routing_key
                and self.schema_name == target_msg_name
            )

        if self.exchange != target_exchange:
            return False

        routing_key_matches = _wildcard_match(target_routing_key, self.routing_key)
        return routing_key_matches and (
            self.schema_name == "" or self.schema_name == target_msg_name
        )


class DefaultBus:
    def __init__(
        self,
        amqp_url: str,
        svc_name: str,
        serializer: Optional[Serializer] = None,
        default_policies: Optional[List[MessagePolicy]] = None,
        worker_num: int = 1,
        prefetch_count: int = 1,
        purge_on_startup: bool = False,
        dlx: Optional[str] = None,
        heartbeat: Optional[int] = 30,
    ):
        self.amqp_url = amqp_url
        self.svc_name = svc_name
        self.serializer = serializer or JsonSerializer()
        self.default_policies = default_policies or []
        self.worker_num = worker_num
        self.prefetch_count = prefetch_count
        self.purge_on_startup = purge_on_startup
        self.dlx = dlx
        # None keeps the URL value; an int is applied to every connection the
        # bus opens so the URL can no longer silently disable heartbeats.
        self.heartbeat = heartbeat

        self._started = False
        self._handlers_lock = threading.Lock()
        self._registrations: List[_Registration] = []
        self._rpc_handlers_lock = threading.Lock()
        self._rpc_handlers: Dict[str, Callable[[BusMessage], None]] = {}
        self._publisher_lock = threading.Lock()
        self._inflight_cond = threading.Condition()
        self._inflight = 0
        self._io_thread: Optional[threading.Thread] = None

        self._consumer_connection: Optional[pika.BlockingConnection] = None
        self._publisher_connection: Optional[pika.BlockingConnection] = None
        self._consumer_channel: Optional[BlockingChannel] = None
        self._publisher_channel: Optional[BlockingChannel] = None
        self._service_queue: Optional[str] = None
        self._rpc_queue: Optional[str] = None
        self._delayed_subscriptions: List[tuple] = []
        self._rpc_consumer_tag: Optional[str] = None
        self._service_consumer_tag: Optional[str] = None

    def register_message_type(self, message_type: Type[Message]) -> None:
        self.serializer.register(message_type)

    def handle_message(self, message_type: Type[Message], handler: MessageHandler) -> None:
        schema_name = message_type().schema_name()
        self.serializer.register(message_type)
        self._register(schema_name, "", self.svc_name, handler)

    def handle_event(
        self,
        exchange: str,
        topic: str,
        message_type: Optional[Type[Message]],
        handler: MessageHandler,
    ) -> None:
        schema_name = None
        if message_type:
            schema_name = message_type().schema_name()
            self.serializer.register(message_type)
        if not self._started:
            self._delayed_subscriptions.append((exchange, topic))
        else:
            self._bind_queue(topic, exchange)
        self._register(schema_name, exchange, topic, handler)

    def _register(
        self,
        schema_name: Optional[str],
        exchange: str,
        routing_key: str,
        handler: MessageHandler,
    ) -> None:
        with self._handlers_lock:
            self._registrations.append(
                _Registration(exchange, routing_key, schema_name, handler)
            )

    def _connection_params(
        self, blocked_timeout: Optional[float] = None
    ) -> pika.ConnectionParameters:
        params = pika.URLParameters(self.amqp_url)
        if self.heartbeat is not None:
            params.heartbeat = self.heartbeat
        if blocked_timeout is not None:
            params.blocked_connection_timeout = blocked_timeout
        return params

    def start(self) -> None:
        if self._started:
            return

        self._consumer_connection = pika.BlockingConnection(
            self._connection_params()
        )
        self._publisher_connection = pika.BlockingConnection(
            self._connection_params()
        )
        self._consumer_channel = self._consumer_connection.channel()
        self._publisher_channel = self._open_publisher_channel()

        if self.prefetch_count:
            self._consumer_channel.basic_qos(prefetch_count=self.prefetch_count)

        self._service_queue = self._declare_service_queue()
        self._declare_and_bind_service_queue()
        self._rpc_queue = self._declare_rpc_queue()

        self._started = True

        # bind delayed subscriptions now that the channel exists
        for exchange, topic in self._delayed_subscriptions:
            self._bind_queue(topic, exchange)

        self._start_consuming()

    def _declare_service_queue(self) -> str:
        args = {}
        if self.dlx:
            args["x-dead-letter-exchange"] = self.dlx

        if self.purge_on_startup:
            try:
                self._consumer_channel.queue_delete(queue=self.svc_name)
            except Exception as e:
                logger.warning("failed to purge queue %s: %s", self.svc_name, e)

        result = self._consumer_channel.queue_declare(
            queue=self.svc_name,
            durable=True,
            auto_delete=False,
            exclusive=False,
            arguments=args or None,
        )
        return result.method.queue

    def _declare_and_bind_service_queue(self) -> None:
        if self.dlx:
            self._consumer_channel.exchange_declare(
                exchange=self.dlx, exchange_type="fanout", durable=True
            )
            self._consumer_channel.queue_bind(
                queue=self._service_queue, exchange=self.dlx, routing_key=""
            )

    def _declare_rpc_queue(self) -> str:
        result = self._consumer_channel.queue_declare(
            queue=f"{self.svc_name}_rpc_{uuid.uuid4().hex}",
            durable=False,
            auto_delete=True,
            exclusive=True,
        )
        return result.method.queue

    def _bind_queue(self, topic: str, exchange: str) -> None:
        self._consumer_channel.exchange_declare(
            exchange=exchange, exchange_type="topic", durable=True
        )
        self._consumer_channel.queue_bind(
            queue=self._service_queue, exchange=exchange, routing_key=topic
        )

    def _start_consuming(self) -> None:
        self._service_consumer_tag = self._consumer_channel.basic_consume(
            queue=self._service_queue,
            on_message_callback=self._on_service_message,
            auto_ack=False,
        )
        self._rpc_consumer_tag = self._consumer_channel.basic_consume(
            queue=self._rpc_queue,
            on_message_callback=self._on_rpc_message,
            auto_ack=False,
        )

        # run I/O loop in a background thread so callers can keep working
        self._io_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self._io_thread.start()

    def _consume_loop(self) -> None:
        while self._started and self._consumer_channel and self._consumer_channel.is_open:
            try:
                self._consumer_connection.process_data_events(time_limit=None)
            except Exception as e:
                if self._started:
                    logger.error("consumer loop error: %s", e)
                break

    def _on_service_message(
        self,
        ch: BlockingChannel,
        method: pika.frame.Method,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        self._process_delivery(ch, method, properties, body, is_rpc_reply=False)

    def _on_rpc_message(
        self,
        ch: BlockingChannel,
        method: pika.frame.Method,
        properties: pika.BasicProperties,
        body: bytes,
    ) -> None:
        self._process_delivery(ch, method, properties, body, is_rpc_reply=True)

    def _process_delivery(
        self,
        ch: BlockingChannel,
        method: pika.frame.Method,
        properties: pika.BasicProperties,
        body: bytes,
        is_rpc_reply: bool,
    ) -> None:
        try:
            bus_message = self._extract_bus_message(properties, body, method.exchange)
        except Exception as e:
            logger.error("failed to decode message: %s", e)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)
            return

        if is_rpc_reply:
            rpc_id = (properties.headers or {}).get("x-logiqbits-rabbitbus-msg-rpc-id")
            with self._rpc_handlers_lock:
                callback = self._rpc_handlers.pop(rpc_id, None)
            if callback:
                callback(bus_message)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        handlers = self._resolve_handlers(
            method.exchange, method.routing_key, bus_message.payload_fqn
        )
        if not handlers:
            logger.warning("no handlers for %s", bus_message.payload_fqn)
            ch.basic_ack(delivery_tag=method.delivery_tag)
            return

        invocation = Invocation(
            bus=self,
            inbound_message=bus_message,
            reply_to=properties.reply_to,
            exchange=method.exchange or "",
            routing_key=method.routing_key or "",
        )

        handler_error: List[BaseException] = []
        handler_done = threading.Event()

        def _run_handlers() -> None:
            try:
                for handler in handlers:
                    handler(invocation, bus_message)
            except Exception as e:
                handler_error.append(e)
            finally:
                handler_done.set()

        # ponytail: pumping process_data_events from inside a delivery callback
        # is safe ONLY because prefetch_count=1 — the broker holds the next
        # message while this one is unacked, so the pump can never dispatch a
        # second delivery mid-job. With prefetch>1 the pump can re-enter this
        # callback on the I/O thread and run handlers concurrently; revisit
        # (serialize dispatch or move handlers onto a single worker thread).
        with self._inflight_cond:
            self._inflight += 1
        try:
            threading.Thread(target=_run_handlers, daemon=True).start()
            while not handler_done.wait(5):
                if not self._started:
                    break
                try:
                    self._consumer_connection.process_data_events(time_limit=0)
                except Exception as e:
                    logger.error("heartbeat pump error: %s", e)
                    break
        finally:
            with self._inflight_cond:
                self._inflight -= 1
                self._inflight_cond.notify_all()

        try:
            if handler_error:
                raise handler_error[0]
            ch.basic_ack(delivery_tag=method.delivery_tag)
        except Exception as e:
            logger.exception("handler error: %s", e)
            ch.basic_reject(delivery_tag=method.delivery_tag, requeue=False)

    def _extract_bus_message(
        self,
        properties: pika.BasicProperties,
        body: bytes,
        exchange: str,
    ) -> BusMessage:
        headers = properties.headers or {}
        bm = BusMessage.from_amqp_headers(headers)
        bm.id = properties.message_id
        bm.correlation_id = properties.correlation_id
        bm.semantics = "evt" if exchange else "cmd"
        bm.payload = self.serializer.decode(body, bm.payload_fqn)
        return bm

    def _resolve_handlers(
        self, exchange: str, routing_key: str, msg_name: str
    ) -> List[MessageHandler]:
        handlers = []
        with self._handlers_lock:
            for reg in self._registrations:
                if reg.matches(exchange, routing_key, msg_name):
                    handlers.append(reg.handler)
        return handlers

    def send(self, to_service: str, message: BusMessage) -> None:
        message.semantics = "cmd"
        self._send_raw("", to_service, message, reply_to=self.svc_name)

    def publish(self, exchange: str, topic: str, message: BusMessage) -> None:
        message.semantics = "evt"
        self._send_raw(exchange, topic, message, reply_to=None)

    def rpc(
        self,
        service: str,
        request: BusMessage,
        timeout: float = 5.0,
    ) -> BusMessage:
        """RPC using a dedicated short-lived connection.

        The consumer connection may be blocked by a long-running handler, so we
        use a fresh connection for the request and its reply instead of relying
        on the shared bus consumer loop.
        """
        rpc_id = uuid.uuid4().hex
        request.rpc_id = rpc_id
        request.semantics = "cmd"

        params = self._connection_params(blocked_timeout=timeout + 10)
        conn = pika.BlockingConnection(params)
        try:
            ch = conn.channel()
            reply_queue = ch.queue_declare(
                queue="", exclusive=True, auto_delete=True
            ).method.queue

            reply_event = threading.Event()
            reply_box: List[BusMessage] = []

            def _on_reply(
                _ch: BlockingChannel,
                _method: pika.frame.Method,
                props: pika.BasicProperties,
                body: bytes,
            ) -> None:
                try:
                    reply_box.append(self._extract_bus_message(props, body, ""))
                except Exception as e:
                    logger.error("failed to decode RPC reply: %s", e)
                reply_event.set()
                _ch.stop_consuming()

            ch.basic_consume(
                queue=reply_queue,
                on_message_callback=_on_reply,
                auto_ack=True,
            )

            body = self.serializer.encode(request.payload)
            properties = pika.BasicProperties(
                message_id=request.id,
                correlation_id=request.correlation_id,
                reply_to=reply_queue,
                content_encoding=self.serializer.name(),
                headers=request.to_amqp_headers(),
                delivery_mode=2,  # persistent
            )
            for policy in self.default_policies:
                policy.apply(properties)

            ch.basic_publish(
                exchange="",
                routing_key=service,
                body=body,
                properties=properties,
            )

            deadline = time.time() + timeout
            while not reply_event.is_set() and time.time() < deadline:
                conn.process_data_events(time_limit=min(1.0, deadline - time.time()))

            if not reply_event.is_set():
                raise TimeoutError(f"RPC call to {service} timed out")

            return reply_box[0]
        finally:
            try:
                conn.close()
            except Exception:
                pass

    def _open_publisher_channel(self) -> BlockingChannel:
        channel = self._publisher_connection.channel()
        # confirm mode makes basic_publish synchronous: it waits for the
        # broker ack and raises UnroutableError/NackError on failure.
        channel.confirm_delivery()
        return channel

    def _ensure_publisher_channel(self) -> BlockingChannel:
        """Return an open publisher channel, reconnecting if necessary.

        The publisher connection can be reset by the broker when it sits idle
        during a long-running handler. Force a health check and reconnect if
        the channel or connection is dead.
        """
        if self._publisher_channel is not None and self._publisher_channel.is_open:
            try:
                if self._publisher_connection is not None:
                    self._publisher_connection.process_data_events(0)
                return self._publisher_channel
            except Exception:
                pass

        if (
            self._publisher_connection is not None
            and self._publisher_connection.is_open
        ):
            try:
                self._publisher_channel = self._open_publisher_channel()
                return self._publisher_channel
            except Exception:
                pass

        try:
            if self._publisher_connection is not None:
                self._publisher_connection.close()
        except Exception:
            pass

        self._publisher_connection = None
        self._publisher_channel = None
        self._publisher_connection = pika.BlockingConnection(
            self._connection_params()
        )
        self._publisher_channel = self._open_publisher_channel()
        logger.warning("rabbitmq publisher reconnected")
        return self._publisher_channel

    def _send_raw(
        self,
        exchange: str,
        routing_key: str,
        message: BusMessage,
        reply_to: Optional[str] = None,
    ) -> None:
        if not self._started:
            raise RuntimeError("bus not started")

        body = self.serializer.encode(message.payload)
        properties = pika.BasicProperties(
            message_id=message.id,
            correlation_id=message.correlation_id,
            reply_to=reply_to,
            content_encoding=self.serializer.name(),
            headers=message.to_amqp_headers(),
            delivery_mode=2,  # persistent
        )
        for policy in self.default_policies:
            policy.apply(properties)

        def _do_publish() -> None:
            channel = self._ensure_publisher_channel()
            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=True,
            )

        with self._publisher_lock:
            try:
                _do_publish()
            except (
                pika.exceptions.UnroutableError,
                pika.exceptions.NackError,
            ) as exc:
                # broker saw the message and refused/returned it; reconnecting
                # and republishing would fail the same way.
                raise PublishError(
                    f"rabbitmq publish not confirmed: {exc!r}"
                ) from exc
            except (TimeoutError, pika.exceptions.AMQPError) as exc:
                logger.warning(
                    "rabbitmq publish failed (%s), reconnecting and retrying once", exc
                )
                try:
                    if (
                        self._publisher_connection is not None
                        and self._publisher_connection.is_open
                    ):
                        self._publisher_connection.close()
                except Exception:
                    pass
                self._publisher_connection = None
                self._publisher_channel = None
                try:
                    _do_publish()
                except Exception as retry_exc:
                    raise PublishError(
                        f"rabbitmq publish failed after reconnect: {retry_exc!r}"
                    ) from retry_exc

    def health(self) -> bool:
        """True when the bus is started and both connections and the
        consumer thread are alive."""
        return (
            self._started
            and self._consumer_connection is not None
            and self._consumer_connection.is_open
            and self._publisher_connection is not None
            and self._publisher_connection.is_open
            and self._io_thread is not None
            and self._io_thread.is_alive()
        )

    def _management_channel(self) -> BlockingChannel:
        """Publisher channel for management ops. Caller must hold
        _publisher_lock; only the publisher connection is safe to touch from
        non-I/O threads (the consumer connection is owned by the I/O thread).
        A dead channel (e.g. passive declare miss) is discarded so the next
        call/publish recreates it."""
        if not self._started:
            raise RuntimeError("bus not started")
        return self._ensure_publisher_channel()

    def queue_depth(self, name: str) -> int:
        """Depth of a queue via passive declare; 0 when the queue is absent."""
        with self._publisher_lock:
            ch = self._management_channel()
            try:
                return ch.queue_declare(
                    queue=name, passive=True
                ).method.message_count
            except pika.exceptions.ChannelClosedByBroker:
                return 0
            finally:
                if not ch.is_open:
                    self._publisher_channel = None

    def purge(self, name: str) -> int:
        """Purge a queue; returns the number of messages removed (0 when the
        queue is absent)."""
        with self._publisher_lock:
            ch = self._management_channel()
            try:
                return ch.queue_purge(queue=name).method.message_count
            except pika.exceptions.ChannelClosedByBroker:
                return 0
            finally:
                if not ch.is_open:
                    self._publisher_channel = None

    def deadletter_count(self) -> int:
        """Depth of the DLX parking queue, 0 when deadlettering is not
        configured or the parking queue does not exist."""
        if not self.dlx:
            return 0
        # ponytail: assumes a parking queue named "<dlx>.parking"; this bus
        # binds the DLX back onto the service queue for redelivery, so there
        # is no parking queue to count unless the operator created one.
        return self.queue_depth(f"{self.dlx}.parking")

    def shutdown(self, drain: bool = False, drain_timeout: float = 30.0) -> None:
        """Stop consuming and close connections.

        With drain=True, cancel the consumers first and let the in-flight
        handler finish (bounded by drain_timeout) so its message is acked
        cleanly instead of being requeued by the broker.
        """
        if drain:
            try:
                if self._consumer_channel and self._consumer_channel.is_open:
                    for tag in (self._service_consumer_tag, self._rpc_consumer_tag):
                        if tag:
                            self._consumer_channel.basic_cancel(tag)
            except Exception:
                pass
            deadline = time.time() + drain_timeout
            with self._inflight_cond:
                while self._inflight > 0 and time.time() < deadline:
                    self._inflight_cond.wait(timeout=0.5)

        self._started = False
        if self._consumer_channel and self._consumer_channel.is_open:
            try:
                self._consumer_channel.stop_consuming()
            except Exception:
                pass
        if self._io_thread is not None:
            self._io_thread.join(timeout=5)
            self._io_thread = None
        if self._consumer_connection and self._consumer_connection.is_open:
            try:
                self._consumer_connection.close()
            except Exception:
                pass
        if self._publisher_connection and self._publisher_connection.is_open:
            try:
                self._publisher_connection.close()
            except Exception:
                pass
