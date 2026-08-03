import logging
import time

from rabbitbus import new, BusMessage, Message, Durable

logging.basicConfig(level=logging.INFO)


class Command1(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "e2e.Command1"


class Reply1(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "e2e.Reply1"


class Event1(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "e2e.Event1"


class RpcRequest(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "e2e.RpcRequest"


class RpcResponse(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "e2e.RpcResponse"


def main():
    conn = "amqp://guest:guest@localhost"
    svc_name = "python.e2e.service"

    bus = (
        new()
        .bus(conn)
        .with_policies(Durable())
        .purge_on_startup()
        .build(svc_name)
    )

    # Handle command from Go and reply back.
    def on_command(invocation, message):
        cmd = message.payload
        print(f"[Python] received command: {cmd.data}")
        invocation.reply(BusMessage(Reply1(data="reply-from-python")))

    bus.handle_message(Command1, on_command)

    # Register response types that this service will receive but not handle directly.
    bus.register_message_type(Reply1)
    bus.register_message_type(RpcResponse)

    # Handle event published by Go.
    def on_event(invocation, message):
        evt = message.payload
        print(f"[Python] received event: {evt.data}")

    bus.handle_event("test.exchange", "test.topic", Event1, on_event)

    # Handle RPC request from Go.
    def on_rpc_request(invocation, message):
        req = message.payload
        print(f"[Python] received RPC request: {req.data}")
        invocation.reply(BusMessage(RpcResponse(data="rpc-response-from-python")))

    bus.handle_message(RpcRequest, on_rpc_request)

    bus.start()
    print("[Python] bus started")

    # 1) Send command to Go service.
    time.sleep(2)
    bus.send("go.e2e.service", BusMessage(Command1(data="cmd-from-python")))
    print("[Python] sent command to go")

    # 2) Publish event that Go subscribes to.
    time.sleep(2)
    bus.publish("test.exchange", "test.topic", BusMessage(Event1(data="event-from-python")))
    print("[Python] published event")

    # 3) RPC to Go service.
    time.sleep(2)
    reply = bus.rpc("go.e2e.service", BusMessage(RpcRequest(data="rpc-from-python")), timeout=5.0)
    resp = reply.payload
    print(f"[Python] received RPC response: {resp.data}")

    # Keep alive to receive messages from Go.
    time.sleep(10)
    bus.shutdown()


if __name__ == "__main__":
    main()
