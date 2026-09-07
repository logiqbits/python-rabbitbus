"""Hardening checks for python-rabbitbus 0.3.0 against a live broker.

Plain assert-based script (no pytest), same convention as e2e_python.py.
Run from the repo root:

    PYTHONPATH=. python tests/test_hardening.py

Uses uniquely-named rbtest_* queues/exchanges and cleans them up in finally.
"""

import sys
import threading
import time
import uuid
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import pika

from rabbitbus import builder, BusMessage, Message, PublishError

AMQP_URL = "amqp://user:Password%402026@119.40.87.181:5672"

SUFFIX = uuid.uuid4().hex[:8]
EXCHANGE = f"rbtest_ex_{SUFFIX}"
QUEUE = f"rbtest_q_{SUFFIX}"
PURGE_QUEUE = f"rbtest_purge_{SUFFIX}"
SVC_PUB = f"rbtest_pub_{SUFFIX}"
SVC_RECV = f"rbtest_recv_{SUFFIX}"
SVC_SEND = f"rbtest_send_{SUFFIX}"


class TMsg(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "rbtest.TMsg"


def make_bus(svc_name, heartbeat=30):
    return builder().bus(AMQP_URL).heartbeat(heartbeat).build(svc_name)


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


def test_heartbeat_pump():
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

        assert bus.deadletter_count() == 0, "no dlx configured -> 0"
        print("PASS deadletter_count() == 0 without dlx")
    finally:
        try:
            bus.shutdown(drain=True)
        except Exception:
            pass
        for q in (QUEUE, PURGE_QUEUE, SVC_PUB, SVC_RECV, SVC_SEND):
            try:
                raw_ch.queue_delete(queue=q)
            except Exception:
                pass
        try:
            raw_ch.exchange_delete(exchange=EXCHANGE)
        except Exception:
            pass
        raw_conn.close()
    print("ALL HARDENING TESTS PASSED")


if __name__ == "__main__":
    main()
