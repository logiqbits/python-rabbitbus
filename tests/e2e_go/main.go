package main

import (
	"context"
	"log"
	"time"

	"github.com/logiqbits/go-rabbitbus/gbus"
	"github.com/logiqbits/go-rabbitbus/gbus/builder"
	"github.com/logiqbits/go-rabbitbus/gbus/policy"
	"github.com/logiqbits/go-rabbitbus/gbus/serialization"
)

type Command1 struct {
	Data string `json:"data"`
}

func (Command1) SchemaName() string { return "e2e.Command1" }

type Reply1 struct {
	Data string `json:"data"`
}

func (Reply1) SchemaName() string { return "e2e.Reply1" }

type Event1 struct {
	Data string `json:"data"`
}

func (Event1) SchemaName() string { return "e2e.Event1" }

type RpcRequest struct {
	Data string `json:"data"`
}

func (RpcRequest) SchemaName() string { return "e2e.RpcRequest" }

type RpcResponse struct {
	Data string `json:"data"`
}

func (RpcResponse) SchemaName() string { return "e2e.RpcResponse" }

func main() {
	conn := "amqp://guest:guest@localhost"
	svcName := "go.e2e.service"

	bus := builder.New().Bus(conn).
		WithSerializer(serialization.NewJsonSerializer()).
		WithPolicies(&policy.Durable{}).
		PurgeOnStartUp().
		Build(svcName)

	// Handle reply to our own command.
	bus.HandleMessage(Reply1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		reply := message.Payload.(*Reply1)
		log.Printf("[Go] received reply: %s", reply.Data)
		return nil
	})

	// Handle command from Python and reply back.
	bus.HandleMessage(Command1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		cmd := message.Payload.(*Command1)
		log.Printf("[Go] received command: %s", cmd.Data)
		return invocation.Reply(context.Background(), gbus.NewBusMessage(Reply1{Data: "reply-from-go"}))
	})

	// Handle event published by Python.
	bus.HandleEvent("test.exchange", "test.topic", Event1{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		evt := message.Payload.(*Event1)
		log.Printf("[Go] received event: %s", evt.Data)
		return nil
	})

	// Handle RPC request from Python.
	bus.HandleMessage(RpcRequest{}, func(invocation gbus.Invocation, message *gbus.BusMessage) error {
		req := message.Payload.(*RpcRequest)
		log.Printf("[Go] received RPC request: %s", req.Data)
		return invocation.Reply(context.Background(), gbus.NewBusMessage(RpcResponse{Data: "rpc-response-from-go"}))
	})

	if err := bus.Start(); err != nil {
		log.Fatal(err)
	}
	defer bus.Shutdown()

	// Give Python service time to start.
	time.Sleep(2 * time.Second)

	// 1) Send command to Python service.
	go func() {
		time.Sleep(1 * time.Second)
		if err := bus.Send(context.Background(), "python.e2e.service", gbus.NewBusMessage(Command1{Data: "cmd-from-go"})); err != nil {
			log.Printf("[Go] failed to send command to python: %v", err)
		}
	}()

	// 2) Publish event that Python subscribes to.
	go func() {
		time.Sleep(2 * time.Second)
		if err := bus.Publish(context.Background(), "test.exchange", "test.topic", gbus.NewBusMessage(Event1{Data: "event-from-go"})); err != nil {
			log.Printf("[Go] failed to publish event: %v", err)
		}
	}()

	// 3) RPC to Python service.
	go func() {
		time.Sleep(3 * time.Second)
		reply, err := bus.RPC(
			context.Background(),
			"python.e2e.service",
			gbus.NewBusMessage(RpcRequest{Data: "rpc-from-go"}),
			gbus.NewBusMessage(RpcResponse{}),
			5*time.Second,
		)
		if err != nil {
			log.Printf("[Go] RPC to python failed: %v", err)
			return
		}
		resp := reply.Payload.(*RpcResponse)
		log.Printf("[Go] received RPC response: %s", resp.Data)
	}()

	// Keep the process alive.
	select {}
}
