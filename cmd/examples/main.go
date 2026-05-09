package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log"
	"maps"
	"net"
	"slices"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbService "github.com/KianIt/gRPC-example/protobuf/service"
)

var (
	address      = ":49000"
	messageCount = 5
)

var example string

func init() {
	flag.StringVar(&example, "example", "all", "Name of example to run")
	flag.Parse()
}

func main() {
	// Сервер.
	log.Printf("Starting gRPC server")

	if err := startServer(address); err != nil {
		log.Fatalf("Error starting gRPC server: %s", err)
	}

	// Клиент.
	log.Printf("Setting up gRPC client")

	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("Error creating connection: %v", err)
	}
	defer func() { _ = conn.Close() }()

	serviceClient := pbService.NewServiceClient(conn)

	// Запуск примеров.
	switch ex, found := exampleMap[example]; {
	case example == "all":
		for _, example = range exampleNames {
			exampleMap[example](serviceClient)
		}
	case found:
		ex(serviceClient)
	default:
		log.Fatalf("Unknown example %s", example)
	}
}

// startServer запускает сервер для сервиса.
func startServer(address string) error {
	linstener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("tcp linstener error: %w", err)
	}

	server := grpc.NewServer()
	pbService.RegisterServiceServer(server, &service{logPrefix: "[SERVER]"})

	go func() {
		log.Printf("Server listening at: %s", address)

		if err = server.Serve(linstener); err != nil {
			log.Fatalf("Serving error: %v", err)
		}
	}()

	time.Sleep(time.Second)

	return nil
}

// exampleMap мапа примеров.
var (
	exampleMap = map[string]func(pbService.ServiceClient){
		"example1": example1,
		"example2": example2,
		"example3": example3,
		"example4": example4,
	}
	exampleNames = slices.Sorted(maps.Keys(exampleMap))
)

// example1 - пример 1: отправка и получение одиночных сообщений.
func example1(serviceClient pbService.ServiceClient) {
	log.Printf("////////////////////////////////////////////////")
	log.Printf("EXAMPLE 1: Sending and receiving simple messages")
	log.Printf("////////////////////////////////////////////////")

	for _, message := range generateMessages(messageCount) {
		clientLogf("Sending message %d", message.GetId())

		if response, err := serviceClient.ProcessMessage(context.Background(), message); err != nil {
			clientLogf("Received error: %v", err)
		} else {
			clientLogf("Received response message")

			processResponseMessage(response)
		}
	}

}

// example2 - пример 2: отправка сообщений в потоке и получение списка сообщений.
func example2(serviceClient pbService.ServiceClient) {
	log.Printf("/////////////////////////////////////////////////////////////////////")
	log.Printf("EXAMPLE 2: Sending requests in stream and returning list of responses")
	log.Printf("/////////////////////////////////////////////////////////////////////")

	stream, err := serviceClient.ProcessMessageRequestStream(context.Background())
	if err != nil {
		log.Fatalf("Error getting request stream: %v", err)
	}

	for _, message := range generateMessages(messageCount) {
		clientLogf("Sending message %d", message.GetId())

		if err = stream.Send(message); err != nil {
			clientLogf("Error sending message %d: %v", message.GetId(), err)
		}
	}

	responses, err := stream.CloseAndRecv()
	if err != nil {
		log.Fatalf("Error closing stream: %v", err)
	}

	clientLogf("Received list of response messages")

	for _, message := range responses.GetMessages() {
		processResponseMessage(message)
	}
}

// example3 - пример 3: отправка списка сообщений и получение сообщений в потоке.
func example3(serviceClient pbService.ServiceClient) {
	log.Printf("/////////////////////////////////////////////////////////////////////")
	log.Printf("Example 3: Sending list of requests and returning responses in stream")
	log.Printf("/////////////////////////////////////////////////////////////////////")

	messages := &pbService.Messages{}
	messages.SetMessages(generateMessages(messageCount))

	clientLogf("Sending list of messages")

	stream, err := serviceClient.ProcessMessageResponseStream(context.Background(), messages)
	if err != nil {
		log.Fatalf("Error getting response stream: %v", err)
	}

	for {
		var response *pbService.Message
		response, err = stream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Fatalf("Error receiving response message: %v", err)
			}
			break
		}

		clientLogf("Received response message")

		processResponseMessage(response)
	}
}

// example4 - пример 4: отправка и получение сообщений в потоке.
func example4(serviceClient pbService.ServiceClient) {
	log.Printf("/////////////////////////////////////////////////////////////")
	log.Printf("EXAMPLE 4: Sending requests and returning responses in stream")
	log.Printf("/////////////////////////////////////////////////////////////")

	stream, err := serviceClient.ProcessMessageBidirectionalStream(context.Background())
	if err != nil {
		log.Fatalf("Error getting stream: %v", err)
	}

	wg := sync.WaitGroup{}
	wg.Go(
		func() {
			for _, message := range generateMessages(messageCount) {
				clientLogf("Sending message %d", message.GetId())

				if e := stream.Send(message); e != nil {
					clientLogf("Error sending message %d: %v", message.GetId(), err)
				}
			}

			if e := stream.CloseSend(); e != nil {
				log.Fatalf("Error closing stream: %v", e)
			}
		},
	)
	defer wg.Wait()

	for {
		var response *pbService.Message
		response, err = stream.Recv()
		if err != nil {
			if err != io.EOF {
				log.Fatalf("Error receiving response message: %v", err)
			}
			break
		}

		clientLogf("Received response message")

		processResponseMessage(response)
	}
}

// generateMessages генерирует сообщения для сервиса.
func generateMessages(count int) []*pbService.Message {
	messages := make([]*pbService.Message, 0, count)

	for i := 1; i <= count; i++ {
		message := &pbService.Message{}
		message.SetId(int32(i))
		message.SetType(pbService.MessageType_MESSAGE_TYPE_REQUEST)
		message.SetTitle(fmt.Sprintf("Request message %d", i))
		message.SetTags([]string{"gRPC", "message", "request"})
		message.SetData([]byte("Request data"))
		if i == messageCount {
			message.SetIsLast(true)
		} else {
			message.SetCountMore(int32(messageCount - i))
		}
		message.SetSentAt(timestamppb.Now())

		messages = append(messages, message)
	}

	return messages
}

// processResponseMessage обрабатывает ответное сообщение от сервиса.
func processResponseMessage(msg *pbService.Message) {
	clientLogf("Message %d of type %s (sent at %v)", msg.GetId(), msg.GetType(), msg.GetSentAt().AsTime())
	clientLogf("Message title: %s", msg.GetTitle())
	clientLogf("Message tags: %v", msg.GetTags())
	clientLogf("Message data: %s", string(msg.GetData()))
	if msg.GetIsLast() {
		clientLogf("Message is last, no more messages expected")
	} else {
		clientLogf("Message is not last, expecting %d more messages", msg.GetCountMore())
	}
}

// clientLogf логирует сообщение от имени клиента.
func clientLogf(format string, args ...any) {
	log.Printf("[CLIENT] %s", fmt.Sprintf(format, args...))
}
