package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"time"

	pbService "github.com/KianIt/gRPC-example/protobuf/service"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// service - имплементация сервиса.
type service struct {
	logPrefix string
	pbService.UnimplementedServiceServer
}

// ProcessMessage обрабатывает входящее сообщение и возвращает ответное сообщение.
func (s *service) ProcessMessage(_ context.Context, msg *pbService.Message) (*pbService.Message, error) {
	s.logf("Processing message")

	return s.processMessage(msg), nil
}

// ProcessMessageRequestStream обрабатывает входящие сообщения из потока и возвращает список ответных сообщений.
func (s *service) ProcessMessageRequestStream(stream grpc.ClientStreamingServer[pbService.Message, pbService.Messages]) error {
	s.logf("Listening message stream")

	rsps := make([]*pbService.Message, 0)
	for {
		s.logf("Receiving message from stream")

		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return status.Errorf(codes.InvalidArgument, "error receiving msg: %v", err)
		}

		s.logf("Processing message")

		rsps = append(rsps, s.processMessage(msg))
	}

	s.logf("Message stream closed")

	rsp := &pbService.Messages{}
	rsp.SetMessages(rsps)

	return stream.SendAndClose(rsp)

}

// ProcessMessageResponseStream обрабатывает список входящих сообщений и возвращает ответные сообщения в потоке.
func (s *service) ProcessMessageResponseStream(msgs *pbService.Messages, stream grpc.ServerStreamingServer[pbService.Message]) error {
	s.logf("Processing messages to stream")

	for _, msg := range msgs.GetMessages() {
		s.logf("Processing message")

		rsp := s.processMessage(msg)

		s.logf("Sending message to stream")

		if err := stream.Send(rsp); err != nil {
			return status.Errorf(codes.Internal, "error sending msg: %v", err)
		}
	}

	return nil
}

// ProcessMessageBidirectionalStream обрабатывает входящие сообщения из потока и возвращает ответные сообщения в потоке.
func (s *service) ProcessMessageBidirectionalStream(stream grpc.BidiStreamingServer[pbService.Message, pbService.Message]) error {
	s.logf("Listening message from and processing messages to stream")

	for {
		s.logf("Receiving message from stream")

		msg, err := stream.Recv()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return status.Errorf(codes.InvalidArgument, "error receiving msg: %v", err)
		}

		s.logf("Processing message")

		rsp := s.processMessage(msg)

		s.logf("Sending message to stream")

		if err = stream.Send(rsp); err != nil {
			return status.Errorf(codes.Internal, "error sending msg: %v", err)
		}
	}

	return nil
}

// processResponseMessage - метод обработки одиночного сообщения.
func (s *service) processMessage(msg *pbService.Message) *pbService.Message {
	// Логируем полученное сообщение.
	s.logf("Message %d of type %s (sent at %v)", msg.GetId(), msg.GetType(), msg.GetSentAt().AsTime())
	s.logf("Message title: %s", msg.GetTitle())
	s.logf("Message tags: %v", msg.GetTags())
	s.logf("Message data: %s", string(msg.GetData()))
	if msg.GetIsLast() {
		s.logf("Message is last, no more messages expected")
	} else {
		s.logf("Message is not last, expecting %d more messages", msg.GetCountMore())
	}

	// Обрабатываем полученное сообщение.
	s.logf("Processing message for %d seconds", msg.GetId())
	time.Sleep(time.Duration(msg.GetId()) * time.Second)

	// Формируем ответное сообщение.
	rsp := &pbService.Message{}
	rsp.SetId(msg.GetId())
	rsp.SetType(pbService.MessageType_MESSAGE_TYPE_RESPONSE)
	rsp.SetTitle(fmt.Sprintf("Response to message %d", msg.GetId()))
	rsp.SetTags([]string{"gRPC", "message", "response"})
	rsp.SetData([]byte("Response data"))
	if msg.GetIsLast() {
		rsp.SetIsLast(true)
	} else {
		rsp.SetCountMore(msg.GetCountMore())
	}
	rsp.SetSentAt(timestamppb.Now())

	return rsp
}

// clientLogf логирует форматную строку от имени сервиса.
func (s *service) logf(format string, args ...any) {
	log.Printf("%s %s", s.logPrefix, fmt.Sprintf(format, args...))
}
