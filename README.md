# python-rabbitbus

A Python service bus for RabbitMQ, wire-compatible with [go-rabbitbus](https://github.com/logiqbits/go-rabbitbus).

It implements the same messaging patterns and AMQP conventions as the Go library so Python services can exchange **commands**, **events**, and **RPC** calls with existing Go services without changing the transport layer.

## Supported patterns

- **Command-Reply** — point-to-point messaging between named services.
- **Publish/Subscribe** — events via durable topic exchanges.
- **RPC** — synchronous request/response over RabbitMQ.

## Requirements

- Python 3.8+
- RabbitMQ 3.8+ or RabbitMQ 4.x

## Installation

From PyPI (after publish):

```bash
pip install logiqbits-rabbitbus
```

For local development:

```bash
git clone https://github.com/logiqbits/python-rabbitbus.git
cd python-rabbitbus
poetry install
```

## Quick start

```python
from rabbitbus import builder, BusMessage, Message, Durable


class Command1(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "example.Command1"


bus = (
    builder()
    .bus("amqp://guest:guest@localhost")
    .with_policies(Durable())
    .build("python.svc")
)


def on_command(invocation, message):
    cmd = message.payload
    print(f"received command: {cmd.data}")
    return None


bus.handle_message(Command1, on_command)
bus.start()

# send a command to another service
bus.send("go.svc", BusMessage(Command1(data="hello")))
```

## Message contract

Every message must implement the `Message` interface by providing a `schema_name()` method. The default `JsonSerializer` wraps the payload in a small envelope:

```json
{
  "schema_name": "example.Command1",
  "payload": { "data": "hello" }
}
```

The `schema_name` is sent as the AMQP header `x-msg-name` and is used to route the message to the correct handler and to deserialize it on the receiving side.

If your service receives a message type only as a reply or RPC response (not as a direct handler), register it with the serializer:

```python
bus.register_message_type(Reply1)
```

## Pattern examples

### Command-Reply

```python
from rabbitbus import builder, BusMessage, Message, Durable


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
    builder()
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

# send a command and (optionally) handle the reply elsewhere
bus.send("another.svc", BusMessage(Command1(data="hello")))
```

### Publish/Subscribe

```python
from rabbitbus import builder, BusMessage, Message, Durable


class OrderCreated(Message):
    def __init__(self, order_id: str = ""):
        self.order_id = order_id

    def schema_name(self) -> str:
        return "example.OrderCreated"


bus = (
    builder()
    .bus("amqp://guest:guest@localhost")
    .with_policies(Durable())
    .build("notifications.svc")
)


def on_order_created(invocation, message):
    event = message.payload
    print(f"order created: {event.order_id}")


bus.handle_event("events", "order.*", OrderCreated, on_order_created)
bus.start()

# publish an event
bus.publish("events", "order.created", BusMessage(OrderCreated(order_id="123")))
```

### RPC

```python
from rabbitbus import builder, BusMessage, Message, Durable


class RpcRequest(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "example.RpcRequest"


class RpcResponse(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "example.RpcResponse"


bus = (
    builder()
    .bus("amqp://guest:guest@localhost")
    .with_policies(Durable())
    .build("python.rpc.svc")
)


def on_rpc(invocation, message):
    req = message.payload
    invocation.reply(BusMessage(RpcResponse(data=f"hello {req.data}")))


bus.handle_message(RpcRequest, on_rpc)
bus.register_message_type(RpcResponse)
bus.start()

# call a remote service and block until a reply arrives
reply = bus.rpc("go.rpc.svc", BusMessage(RpcRequest(data="world")), timeout=5.0)
print(reply.payload.data)
```

## Interoperability with Go

The Python library defaults to JSON serialization. To make a Go service talk to Python, configure it with the JSON serializer and use matching `SchemaName` values and JSON tags.

### Python message

```python
class Command1(Message):
    def __init__(self, data: str = ""):
        self.data = data

    def schema_name(self) -> str:
        return "example.Command1"
```

### Equivalent Go message

```go
type Command1 struct {
    Data string `json:"data"`
}

func (Command1) SchemaName() string { return "example.Command1" }
```

### Go bus configured for Python interop

```go
import (
    "github.com/logiqbits/go-rabbitbus/gbus"
    "github.com/logiqbits/go-rabbitbus/gbus/builder"
    "github.com/logiqbits/go-rabbitbus/gbus/policy"
    "github.com/logiqbits/go-rabbitbus/gbus/serialization"
)

bus := builder.New().Bus("amqp://guest:guest@localhost").
    WithSerializer(serialization.NewJsonSerializer()).
    WithPolicies(&policy.Durable{}).
    Build("go.svc")
```

After that, `bus.Send(...)`, `bus.Publish(...)`, and `bus.RPC(...)` from Go will interoperate with the Python implementation.

## Configuration

The builder supports the same style of configuration as the Go library:

```python
bus = (
    builder()
    .bus("amqp://guest:guest@localhost")
    .with_policies(Durable())
    .worker_num(workers=4, prefetch_count=10)
    .purge_on_startup()
    .with_deadlettering("dead-letter-exchange")
    .build("python.svc")
)
```

| Builder method | Description |
|---|---|
| `bus(url)` | AMQP connection URL. |
| `with_policies(*policies)` | Default policies applied to every outgoing message (e.g., `Durable`). |
| `worker_num(workers, prefetch_count)` | Number of consumer workers and AMQP prefetch count. |
| `purge_on_startup()` | Delete the service queue on startup. |
| `with_deadlettering(exchange)` | Route rejected/poison messages to the given exchange. |
| `with_serializer(serializer)` | Use a custom serializer instead of the default JSON serializer. |

## Development

Run the bidirectional Go ↔ Python e2e test against a local RabbitMQ broker:

```bash
# terminal 1: start the Go service
cd tests/e2e_go
go run .

# terminal 2: run the Python service/client
cd /Users/rafiul/Documents/workspace/internal/python-rabbitbus
poetry run python tests/e2e_python.py
```

The test exercises Command-Reply, Pub/Sub, and RPC in both directions.

## Publishing to PyPI

Releases are published automatically by GitHub Actions when a GitHub release is created.

For a manual release:

```bash
poetry config pypi-token.pypi <PYPI_API_TOKEN>
poetry publish --build
```

PyPI does not allow re-uploading the same version, so bump the version in `pyproject.toml` if a publish fails.

## License

Apache License 2.0
