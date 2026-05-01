package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/emptypb"
	"google.golang.org/protobuf/types/known/timestamppb"

	pbService "github.com/KianIt/gRPC-example/protobuf/service"
)

var (
	address      = ":49000"
	messageCount = 5
)

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

	service := pbService.NewServiceClient(conn)

	// Отправка отдельных сообщений.
	log.Printf("[CLIENT] Sending messages to %s", address)

	for i := 1; i <= messageCount; i++ {
		message := &pbService.Message{}
		message.SetId(int32(i))
		if i%2 == 0 {
			message.SetType(pbService.MessageType_MESSAGE_TYPE_ALERT)
		}
		message.SetTitle("title")
		message.SetTags([]string{"tag1", "tag2"})
		message.SetData([]byte("data"))
		if i == messageCount {
			message.SetIsLast(true)
		} else {
			message.SetCountMore(int32(messageCount - i))
		}
		message.SetSentAt(timestamppb.Now())

		log.Printf("[CLIENT] Sending message %d: %v", i, message)

		ctx, cancel := context.WithTimeout(context.Background(), time.Duration(i+1)*time.Second)

		if _, err = service.ProcessMessage(ctx, message); err != nil {
			log.Printf("[CLIENT] Error processing message: %v", err)
		} else {
			log.Printf("[CLIENT] Message successfully processed")
		}

		cancel()
	}

	// Стриминг сообщений.
	log.Printf("[CLIENT] Streaming messages to %s", address)

	stream, err := service.ProcessMessageStream(context.Background())
	if err != nil {
		log.Fatalf("Error getting stream: %v", err)
	}

	for i := 1; i <= messageCount; i++ {
		message := &pbService.Message{}
		message.SetId(int32(i))
		if i%2 == 0 {
			message.SetType(pbService.MessageType_MESSAGE_TYPE_ALERT)
		}
		message.SetTitle("title")
		message.SetTags([]string{"tag1", "tag2"})
		message.SetData([]byte("data"))
		if i == messageCount {
			message.SetIsLast(true)
		} else {
			message.SetCountMore(int32(messageCount - i))
		}
		message.SetSentAt(timestamppb.Now())

		log.Printf("[CLIENT] Sending message %d: %v", i, message)

		if err = stream.Send(message); err != nil {
			log.Printf("[CLIENT] Error sending message: %v", err)
		} else {
			log.Printf("[CLIENT] Message successfully sent")
		}
	}

	if _, err = stream.CloseAndRecv(); err != nil {
		log.Fatalf("Error closing stream: %v", err)
	}
}

type serviceImplement struct {
	pbService.UnimplementedServiceServer
}

func (s *serviceImplement) ProcessMessage(_ context.Context, message *pbService.Message) (*emptypb.Empty, error) {
	if message.GetType() == pbService.MessageType_MESSAGE_TYPE_INFO {
		log.Printf("[SERVER] Received INFO message %d (ts: %v)", message.GetId(), message.GetSentAt().AsTime())
	} else {
		log.Printf("[SERVER] Received ALERT message %d (ts: %v)", message.GetId(), message.GetSentAt().AsTime())
	}

	log.Printf("[SERVER] Message '%s': %s", message.GetTitle(), string(message.GetData()))

	if message.GetIsLast() {
		log.Printf("[SERVER] No more messages expected")
	} else {
		log.Printf("[SERVER] Expecting %d more messages", message.GetCountMore())
	}

	log.Println("[SERVER] Processing message")
	time.Sleep(time.Duration(message.GetId()) * time.Second)

	return &emptypb.Empty{}, nil
}

func (s *serviceImplement) ProcessMessageStream(stream grpc.ClientStreamingServer[pbService.Message, emptypb.Empty]) error {
	for {
		message, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return status.Errorf(codes.InvalidArgument, "error receiving message: %v", err)
		}

		if message.GetType() == pbService.MessageType_MESSAGE_TYPE_INFO {
			log.Printf("[SERVER] Received INFO message %d (ts: %v)", message.GetId(), message.GetSentAt().AsTime())
		} else {
			log.Printf("[SERVER] Received ALERT message %d (ts: %v)", message.GetId(), message.GetSentAt().AsTime())
		}

		log.Printf("[SERVER] Message '%s': %s", message.GetTitle(), string(message.GetData()))

		if message.GetIsLast() {
			log.Printf("[SERVER] No more messages expected")
		} else {
			log.Printf("[SERVER] Expecting %d more messages", message.GetCountMore())
		}

		log.Println("[SERVER] Processing message")
		time.Sleep(time.Duration(message.GetId()) * time.Second)
	}

	log.Printf("[SERVER] Message stream closed")

	return stream.SendAndClose(&emptypb.Empty{})
}

func startServer(address string) error {
	linstener, err := net.Listen("tcp", address)
	if err != nil {
		return fmt.Errorf("tcp linstener: %w", err)
	}

	server := grpc.NewServer()

	pbService.RegisterServiceServer(server, &serviceImplement{})

	log.Printf("[SERVER] Server listening at: %s", address)

	go func() {
		if err = server.Serve(linstener); err != nil {
			log.Fatalf("[SERVER] Serving error: %v", err)
		}
	}()

	return nil
}
