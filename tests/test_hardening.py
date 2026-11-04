"""Hardening checks for python-rabbitbus against a live broker.

Plain assert-based script (no pytest), same convention as e2e_python.py.
Run from the repo root:

    PYTHONPATH=. python tests/test_hardening.py

Broker via RABBITBUS_AMQP (default amqp://guest:guest@localhost), same
convention as the Go twin. Uses uniquely-named rbtest_* queues/exchanges and
cleans them up in finally.
"""

import json
import os
import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pika

from rabbitbus import builder, BusMessage, Message, PublishError, retry_exchange_name

AMQP_URL = os.environ.get("RABBITBUS_AMQP", "amqp://guest:guest@localhost")

SUFFIX = uuid.uuid4().hex[:8]
EXCHANGE = f"rbtest_ex_{SUFFIX}"
QUEUE = f"rbtest_q_{SUFFIX}"
PURGE_QUEUE = f"rbtest_purge_{SUFFIX}"
SVC_PUB = f"rbtest_pub_{SUFFIX}"
SVC_RECV = f"rbtest_recv_{SUFFIX}"
SVC_SEND = f"rbtest_send_{SUFFIX}"
SVC_DL = f"rbtest_svc_dl_{SUFFIX}"
DLX = f"rbtest_dlx_{SUFFIX}"
RETRY_EX = retry_exchange_name(SVC_DL)


class TMsg(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "rbtest.TMsg"


def make_bus(svc_name, heartbeat=30):
    return builder().bus(AMQP_URL).heartbeat(heartbeat).build(svc_name)


def make_dl_bus(svc_name, max_attempts=0, delays=None, dlx=DLX):
    b = builder().bus(AMQP_URL).with_deadlettering(dlx)
    if max_attempts:
        b = b.with_deadletter_retries(max_attempts)
    if delays:
        b = b.with_delayed_retry(delays)
    return b.build(svc_name)


def test_publish_confirm_and_unroutable(bus, raw_ch):
    raw_ch.exchange_declare(exchange=EXCHANGE, exchange_type="topic", durable=False)
    raw_ch.queue_declare(queue=QUEUE, durable=False, auto_delete=True)
    raw_ch.queue_bind(queue=QUEUE, exchange=EXCHANGE, routing_key="ok")

    # routed publish returns only after broker confirm
    bus.publish(EXCHANGE, "ok", BusMessage(TMsg(data="one")))
    assert bus.queue_depth(QUEUE) == 1, "confirmed publish must land in the queue"

    # no binding for this routing key: mandatory return -> PublishError
    try:
        bus.publish(EXCHANGE, "nobinding", BusMessage(TMsg(data="lost?")))
        raise AssertionError("expected PublishError for unroutable message")
    except PublishError:
        pass

    # missing exchange entirely: also PublishError, never silent
    try:
        bus.publish(f"rbtest_missing_{SUFFIX}", "ok", BusMessage(TMsg(data="x")))
        raise AssertionError("expected PublishError for missing exchange")
    except PublishError:
        pass

    assert bus.queue_depth(QUEUE) == 1, "no ghost messages from failed publishes"
    print("PASS publish confirm round-trip + unroutable raises PublishError")


def test_retry_reconnect(bus):
    # 1) dead channel underneath: publish must survive via new channel
    bus._publisher_channel.close()
    bus.publish(EXCHANGE, "ok", BusMessage(TMsg(data="two")))
    assert bus.queue_depth(QUEUE) == 2

    # 2) dead connection underneath: publish must reconnect and succeed
    bus._publisher_connection.close()
    bus.publish(EXCHANGE, "ok", BusMessage(TMsg(data="three")))
    assert bus.queue_depth(QUEUE) == 3

    # 3) reconnect happens but republish still fails -> PublishError, not silent
    bus._publisher_connection.close()
    try:
        bus.publish(EXCHANGE, "nobinding", BusMessage(TMsg(data="x")))
        raise AssertionError("expected PublishError after failed reconnect retry")
    except PublishError:
        pass
    print("PASS retry path: dead channel/connection reconnects, failures raise")


def test_heartbeat_pump(_needs_broker=None):
    done = threading.Event()
    started_at = time.time()

    def slow_handler(invocation, message):
        time.sleep(16)  # > 3x the 5s heartbeat; broker would drop us without the pump
        done.set()

    recv = make_bus(SVC_RECV, heartbeat=5)
    recv.handle_message(TMsg, slow_handler)
    recv.start()
    send = make_bus(SVC_SEND, heartbeat=5)
    send.register_message_type(TMsg)
    send.start()
    try:
        send.send(SVC_RECV, BusMessage(TMsg(data="slow")))
        assert done.wait(timeout=40), "slow handler must complete"
        assert time.time() - started_at >= 15, "handler should have actually blocked"
        # the consumer connection survived the 16s block (broker drops at ~2x heartbeat)
        assert recv._consumer_connection.is_open, "consumer connection died mid-job"
        assert recv.health(), "bus must report healthy after slow job"
        # the ack landed: service queue drains back to zero
        deadline = time.time() + 10
        while recv.queue_depth(SVC_RECV) != 0 and time.time() < deadline:
            time.sleep(0.2)
        assert recv.queue_depth(SVC_RECV) == 0, "message must be acked after job"
    finally:
        send.shutdown()
        recv.shutdown(drain=True)
    print("PASS heartbeat pump: connection survives 3x-heartbeat handler block")


def test_consumer_reconnect(_needs_broker=None):
    """Consumer connection drop must not kill the bus: the I/O thread reconnects,
    re-attaches to the durable service queue, and keeps receiving."""
    got = []
    alive = threading.Event()

    def handler(invocation, message):
        got.append(message.payload.data)
        alive.set()

    recv = make_bus(SVC_RECV)
    recv.handle_message(TMsg, handler)
    recv.start()
    send = make_bus(SVC_SEND)
    send.register_message_type(TMsg)
    send.start()
    try:
        send.send(SVC_RECV, BusMessage(TMsg(data="before")))
        assert alive.wait(timeout=15), "first message must be received"
        alive.clear()

        old_conn = recv._consumer_connection
        old_conn.close()  # same trick as test_retry_reconnect for the publisher
        deadline = time.time() + 15
        while recv._consumer_connection is old_conn and time.time() < deadline:
            time.sleep(0.2)
        assert recv._consumer_connection is not old_conn, "consumer must reconnect"

        send.send(SVC_RECV, BusMessage(TMsg(data="after")))
        assert alive.wait(timeout=15), "message after drop must be received"
        assert got == ["before", "after"], f"no duplicates/losses: {got}"
        assert recv.health(), "bus must report healthy after consumer reconnect"
    finally:
        send.shutdown()
        recv.shutdown(drain=True)
    print("PASS consumer reconnect: bus survives a dead consumer connection")


def test_queue_depth_and_purge(bus, raw_ch):
    raw_ch.queue_declare(queue=PURGE_QUEUE, durable=False, auto_delete=True)
    for i in range(3):
        raw_ch.basic_publish(
            exchange="", routing_key=PURGE_QUEUE, body=b"{}",
            properties=pika.BasicProperties(delivery_mode=2),
        )
    time.sleep(0.5)
    assert bus.queue_depth(PURGE_QUEUE) == 3
    assert bus.purge(PURGE_QUEUE) == 3
    assert bus.queue_depth(PURGE_QUEUE) == 0
    print("PASS queue_depth/purge round-trip")


def test_deadletter_callback_once_no_loop():
    """Handler error -> DLX -> poison detected -> deadletter callback exactly
    once, acked; the old code re-ran the handler at full speed forever."""
    handler_calls = []
    dl_calls = []
    got_dl = threading.Event()

    def handler(invocation, message):
        handler_calls.append(message.payload.data)
        raise RuntimeError("boom")

    def on_deadletter(delivery_info, body):
        dl_calls.append(delivery_info)
        got_dl.set()

    recv = make_dl_bus(SVC_DL)
    recv.handle_message(TMsg, handler)
    recv.on_deadletter(on_deadletter)
    recv.start()
    send = make_bus(SVC_SEND)
    send.register_message_type(TMsg)
    send.start()
    try:
        send.send(SVC_DL, BusMessage(TMsg(data="poison")))
        assert got_dl.wait(timeout=15), "deadletter callback must be invoked"
        time.sleep(3)  # long enough for the old infinite loop to explode
        assert len(dl_calls) == 1, f"callback must fire exactly once: {len(dl_calls)}"
        assert len(handler_calls) == 1, "poison must not reach handlers again"
        info = dl_calls[0]
        assert info["exchange"] == DLX, "delivery_info carries the DLX exchange"
        assert info["routing_key"] == SVC_DL
        assert "x-death" in info["headers"], "delivery_info carries x-death"
        assert info["headers"]["x-death"][0]["queue"] == SVC_DL
        deadline = time.time() + 10
        while recv.queue_depth(SVC_DL) != 0 and time.time() < deadline:
            time.sleep(0.2)
        assert recv.queue_depth(SVC_DL) == 0, "acked poison must drain"
    finally:
        send.shutdown()
        recv.shutdown(drain=True)
    print("PASS deadletter: poison routed to callback once, acked, no loop")


def test_deadletter_max_attempts_parks():
    """max_attempts=2: callback fails once -> count reaches 2 -> parked:
    acked, no further callback invocations, no reject re-loop."""
    dl_calls = []
    parked = threading.Event()

    def handler(invocation, message):
        raise RuntimeError("boom")

    def on_deadletter(delivery_info, body):
        dl_calls.append(1)
        raise RuntimeError("cannot persist")

    import rabbitbus.bus as bus_mod
    orig_error = bus_mod.logger.error
    def _watch_park(msg, *args, **kwargs):
        if "parked" in str(msg):
            parked.set()
        orig_error(msg, *args, **kwargs)
    bus_mod.logger.error = _watch_park

    recv = make_dl_bus(SVC_DL, max_attempts=2)
    recv.handle_message(TMsg, handler)
    recv.on_deadletter(on_deadletter)
    recv.start()
    send = make_bus(SVC_SEND)
    send.register_message_type(TMsg)
    send.start()
    try:
        send.send(SVC_DL, BusMessage(TMsg(data="poison")))
        assert parked.wait(timeout=15), "message must be parked at the cap"
        time.sleep(3)
        assert len(dl_calls) == 1, f"callback must not run again: {len(dl_calls)}"
        assert recv.queue_depth(SVC_DL) == 0, "parked message is acked away"
    finally:
        bus_mod.logger.error = orig_error
        send.shutdown()
        recv.shutdown(drain=True)
    print("PASS deadletter cap: max_attempts parks the message, no re-loop")


def test_delayed_retry():
    """Transient failure republished to the retry exchange is redelivered
    into the NORMAL handler after the TTL; foreign-queue x-death falls
    through instead of hitting the deadletter path."""
    seen = []
    done = threading.Event()
    first_at = []

    def handler(invocation, message):
        data = message.payload.data
        seen.append(data)
        if not first_at:
            first_at.append(time.time())
        if data == "first":
            invocation.publish(
                retry_exchange_name(SVC_DL), "", BusMessage(TMsg(data="second"))
            )
            raise RuntimeError("transient")
        done.set()

    recv = make_dl_bus(SVC_DL, delays=[2])
    recv.handle_message(TMsg, handler)
    # the original "first" delivery is rejected after republishing; swallow it
    # as poison so it does not loop into the DLX while the test runs
    recv.on_deadletter(lambda info, body: None)
    recv.start()
    send = make_bus(SVC_SEND)
    send.register_message_type(TMsg)
    send.start()
    try:
        send.send(SVC_DL, BusMessage(TMsg(data="first")))
        assert done.wait(timeout=20), "retried message must reach the handler"
        assert seen == ["first", "second"], f"no extra deliveries: {seen}"
        assert time.time() - first_at[0] >= 1.5, "redelivery must be delayed ~2s"
    finally:
        send.shutdown()
        recv.shutdown(drain=True)
    print("PASS delayed retry: republished failure redelivered after ~2s")


def test_foreign_xdeath_goes_to_handlers():
    """An x-death naming another queue (e.g. dead-lettered from a retry
    queue) arriving at the service exchange is normal work, not poison."""
    got = threading.Event()
    dl_calls = []

    def handler(invocation, message):
        got.set()

    def on_deadletter(delivery_info, body):
        dl_calls.append(1)

    recv = make_dl_bus(SVC_DL)
    recv.handle_message(TMsg, handler)
    recv.on_deadletter(on_deadletter)
    recv.start()
    raw_conn = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    raw_ch = raw_conn.channel()
    try:
        x_death = [{
            "count": 1,
            "exchange": "somewhere",
            "queue": f"rbtest_foreign_{SUFFIX}",
            "reason": "expired",
            "routing-keys": ["other"],
            "time": int(time.time()),
        }]
        body = json.dumps(
            {"schema_name": "rbtest.TMsg", "payload": {"data": "foreign"}}
        ).encode()
        raw_ch.basic_publish(
            exchange="",
            routing_key=SVC_DL,
            body=body,
            properties=pika.BasicProperties(
                headers={"x-msg-name": "rbtest.TMsg", "x-death": x_death},
            ),
        )
        assert got.wait(timeout=15), "foreign x-death must hit normal handlers"
        time.sleep(1)
        assert not dl_calls, "foreign x-death must not invoke the deadletter cb"
    finally:
        raw_conn.close()
        recv.shutdown(drain=True)
    print("PASS foreign x-death: normal handlers, deadletter cb untouched")


def main():
    raw_conn = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    raw_ch = raw_conn.channel()
    bus = make_bus(SVC_PUB)
    try:
        bus.start()
        assert bus.health()

        test_publish_confirm_and_unroutable(bus, raw_ch)
        test_retry_reconnect(bus)
        test_queue_depth_and_purge(bus, raw_ch)
        test_heartbeat_pump()
        test_consumer_reconnect()
        test_deadletter_callback_once_no_loop()
        test_deadletter_max_attempts_parks()
        test_delayed_retry()
        test_foreign_xdeath_goes_to_handlers()

        assert bus.deadletter_count() == 0, "no dlx configured -> 0"
        print("PASS deadletter_count() == 0 without dlx")
    finally:
        try:
            bus.shutdown(drain=True)
        except Exception:
            pass
        for q in (
            QUEUE, PURGE_QUEUE, SVC_PUB, SVC_RECV, SVC_SEND, SVC_DL,
            f"{SVC_DL}.retry.2s",
        ):
            try:
                raw_ch.queue_delete(queue=q)
            except Exception:
                pass
        for ex in (EXCHANGE, DLX, RETRY_EX):
            try:
                raw_ch.exchange_delete(exchange=ex)
            except Exception:
                pass
        raw_conn.close()
    print("ALL HARDENING TESTS PASSED")


if __name__ == "__main__":
    main()


# --- pytest-only section: fixtures for the broker tests above + unit tests ---

import ssl  # noqa: E402
from unittest.mock import MagicMock  # noqa: E402

import pytest  # noqa: E402

import rabbitbus.bus as bus_mod  # noqa: E402
from rabbitbus.bus import DefaultBus  # noqa: E402

UNIT_URL = "amqp://guest:guest@localhost:5672/%2f"


def _broker_available() -> bool:
    try:
        params = pika.URLParameters(AMQP_URL)
        params.socket_timeout = 2
        conn = pika.BlockingConnection(params)
        conn.close()
        return True
    except Exception:
        return False


@pytest.fixture
def _needs_broker():
    if not _broker_available():
        pytest.skip("no live RabbitMQ broker")


@pytest.fixture
def raw_ch():
    if not _broker_available():
        pytest.skip("no live RabbitMQ broker")
    conn = pika.BlockingConnection(pika.URLParameters(AMQP_URL))
    ch = conn.channel()
    yield ch
    conn.close()


@pytest.fixture
def bus():
    if not _broker_available():
        pytest.skip("no live RabbitMQ broker")
    b = make_bus(SVC_PUB)
    b.start()
    yield b
    b.shutdown(drain=True)


class _FakeTime:
    """Stubs sleep (backoff would otherwise take minutes); real time()."""

    def __init__(self):
        self.slept = []

    def sleep(self, seconds):
        self.slept.append(seconds)

    def time(self):
        return time.time()


def test_connection_params_blocked_timeout_default_and_override():
    bus = DefaultBus(UNIT_URL, "svc")
    assert bus._connection_params().blocked_connection_timeout == 300.0
    assert bus._connection_params(None).blocked_connection_timeout == 300.0
    assert bus._connection_params(12.0).blocked_connection_timeout == 12.0
    # rpc() passes timeout + 10 explicitly; that override must win
    assert bus._connection_params(15.0).blocked_connection_timeout == 15.0
    no_block = DefaultBus(UNIT_URL, "svc", blocked_timeout=None)
    assert no_block._connection_params().blocked_connection_timeout is None


def test_start_retries_then_succeeds(monkeypatch):
    fake_time = _FakeTime()
    monkeypatch.setattr(bus_mod, "time", fake_time)
    calls = {"n": 0}
    conn = MagicMock()

    def flaky_connect(params):
        calls["n"] += 1
        if calls["n"] < 3:
            raise pika.exceptions.AMQPConnectionError("broker down")
        return conn

    monkeypatch.setattr(bus_mod.pika, "BlockingConnection", flaky_connect)
    bus = DefaultBus(UNIT_URL, "svc")
    try:
        bus.start()
        assert calls["n"] >= 3  # 2 failures then success; the I/O loop may add more
        assert bus._started
        # consumer + publisher got their blocked/unblocked callbacks
        assert conn.add_on_connection_blocked_callback.call_count >= 2
        assert conn.add_on_connection_unblocked_callback.call_count >= 2
    finally:
        bus.shutdown()


def test_start_retry_bounded_exhaustion(monkeypatch):
    fake_time = _FakeTime()
    monkeypatch.setattr(bus_mod, "time", fake_time)
    attempts = {"n": 0}

    def always_fail(params):
        attempts["n"] += 1
        raise pika.exceptions.AMQPConnectionError("broker down")

    monkeypatch.setattr(bus_mod.pika, "BlockingConnection", always_fail)
    bus = DefaultBus(UNIT_URL, "svc")
    with pytest.raises(pika.exceptions.AMQPConnectionError):
        bus.start()
    assert attempts["n"] == 10  # bounded: not an infinite retry
    assert not bus._started


def test_builder_tls_sets_ssl_options_and_amqps(monkeypatch, tmp_path):
    # stub the file-loading calls; we only assert the wiring, not real certs
    monkeypatch.setattr(
        ssl.SSLContext, "load_verify_locations", lambda self, f: None
    )
    monkeypatch.setattr(
        ssl.SSLContext, "load_cert_chain", lambda self, cf, kf=None: None
    )
    bus = (
        builder()
        .bus("amqp://user:pass@rmq.internal:5672/vhost")
        .tls(
            ca_certs=str(tmp_path / "ca.pem"),
            certfile=str(tmp_path / "client.pem"),
            keyfile=str(tmp_path / "client.key"),
            server_hostname="rmq.internal",
        )
        .build("svc")
    )
    assert bus.amqp_url.startswith("amqps://")
    params = bus._connection_params()
    assert isinstance(params.ssl_options, pika.SSLOptions)
    assert params.ssl_options.server_hostname == "rmq.internal"


def test_builder_tls_before_bus_still_amqps(monkeypatch):
    monkeypatch.setattr(ssl.SSLContext, "load_verify_locations", lambda self, f: None)
    bus = builder().tls(ca_certs="/ca.pem").bus(UNIT_URL).build("svc")
    assert bus.amqp_url.startswith("amqps://")


def test_builder_no_tls_untouched():
    bus = builder().bus(UNIT_URL).build("svc")
    assert bus.amqp_url == UNIT_URL
    assert bus._connection_params().ssl_options is None


def test_builder_blocked_timeout():
    bus = builder().bus(UNIT_URL).blocked_timeout(7).build("svc")
    assert bus._connection_params().blocked_connection_timeout == 7


def test_attach_blocked_callbacks_registers_both():
    bus = DefaultBus(UNIT_URL, "svc")
    conn = MagicMock()
    bus._attach_blocked_callbacks(conn)
    conn.add_on_connection_blocked_callback.assert_called_once()
    conn.add_on_connection_unblocked_callback.assert_called_once()
