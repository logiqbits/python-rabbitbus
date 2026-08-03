# python-rabbitbus

A Python service bus for RabbitMQ, wire-compatible with [go-rabbitbus](https://github.com/logiqbits/go-rabbitbus).

## Supported patterns

- **Command-Reply** — point-to-point messaging between named services.
- **Publish/Subscribe** — events via topic exchanges.
- **RPC** — synchronous request/response over RabbitMQ.

## Install

```bash
poetry install
```

## Quick example

```python
from rabbitbus import new, BusMessage, Message, Durable


class Command1(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "example.Command1"


class Reply1(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "example.Reply1"


bus = (
    new()
    .bus("amqp://guest:guest@localhost")
    .with_policies(Durable())
    .build("python.svc")
)


def on_command(invocation, message):
    cmd = message.payload
    print(f"received: {cmd.data}")
    invocation.reply(BusMessage(Reply1(data="ok")))


bus.handle_message(Command1, on_command)
bus.start()

# send a command to another service
bus.send("go.svc", BusMessage(Command1(data="hello")))

# publish an event
bus.publish("events", "order.created", BusMessage(Command1(data="hello")))

# rpc
reply = bus.rpc("go.svc", BusMessage(Command1(data="ping")), timeout=5.0)
print(reply.payload.data)
```

## Interoperability with Go

The default serializer is JSON. To communicate with a Go service, configure the Go bus to use the JSON serializer:

```go
bus := builder.New().Bus(conn).
    WithSerializer(serialization.NewJsonSerializer()).
    Build("go.svc")
```

Both sides must use matching `schema_name` values and compatible JSON field names.

## Running the bidirectional e2e test

Start RabbitMQ on `amqp://guest:guest@localhost`, then run:

```bash
# terminal 1
cd tests/e2e_go
go run .

# terminal 2
cd ../..
poetry run python tests/e2e_python.py
```

The test exercises Command-Reply, Pub/Sub, and RPC in both directions between Go and Python.
