# gRPC-example

A personal project for learning and practicing the **gRPC** framework

## What's been done

1. Learnt how to use **gRPC** and **Protobuf**
    - how to define new **Protobuf** messages with numerous fields of various types,
    - how to define new **Protobuf** RPC services with message arguments and results,
    - how to automatically generate **gRPC** service code for Golang applications,
    - how to run a server with a **gRPC** service and how to call it via a client,
    - how to send messages via streams in client-side, server-side, and bidirectional streaming.

2. Written an example **gRPC** service application:
    - added `.protobuf` files for a message and a service that uses this message,
    - automatically generated source code for a **gRPC** service in Golang,
    - created own implementation for the non-implemented generated service,
    - created examples of calling service using simple messages and every type of streaming.

## About examples

### What do they show

Every example show one of client-server interaction type: a simple message exchange or message streaming.

Examples demonstrate how and when the client and the server receive and process messages. 
All the interactions can be observed since the client and the server write logs to the standard output.

For example, logs of the `example3`:

```
2026/05/12 10:43:49 Starting gRPC server
2026/05/12 10:43:49 Server listening at: :49000
2026/05/12 10:43:50 Setting up gRPC client
2026/05/12 10:43:50 /////////////////////////////////////////////////////////////////////
2026/05/12 10:43:50 Example 3: Sending list of requests and returning responses in stream
2026/05/12 10:43:50 /////////////////////////////////////////////////////////////////////
2026/05/12 10:43:50 [CLIENT] Sending list of messages
2026/05/12 10:43:50 [SERVER] Processing messages to stream
2026/05/12 10:43:50 [SERVER] Processing message
2026/05/12 10:43:50 [SERVER] Message 1 of type MESSAGE_TYPE_REQUEST (sent at 2026-05-12 07:43:50.248803307 +0000 UTC)
2026/05/12 10:43:50 [SERVER] Message title: Request message 1
2026/05/12 10:43:50 [SERVER] Message tags: [gRPC message request]
2026/05/12 10:43:50 [SERVER] Message data: Request data
2026/05/12 10:43:50 [SERVER] Message is not last, expecting 4 more messages
2026/05/12 10:43:50 [SERVER] Processing message for 1 seconds
2026/05/12 10:43:51 [SERVER] Sending message to stream
2026/05/12 10:43:51 [SERVER] Processing message
2026/05/12 10:43:51 [SERVER] Message 2 of type MESSAGE_TYPE_REQUEST (sent at 2026-05-12 07:43:50.248803946 +0000 UTC)
2026/05/12 10:43:51 [SERVER] Message title: Request message 2
2026/05/12 10:43:51 [SERVER] Message tags: [gRPC message request]
2026/05/12 10:43:51 [SERVER] Message data: Request data
2026/05/12 10:43:51 [SERVER] Message is not last, expecting 3 more messages
2026/05/12 10:43:51 [SERVER] Processing message for 2 seconds
2026/05/12 10:43:51 [CLIENT] Received response message
2026/05/12 10:43:51 [CLIENT] Message 1 of type MESSAGE_TYPE_RESPONSE (sent at 2026-05-12 07:43:51.275823206 +0000 UTC)
2026/05/12 10:43:51 [CLIENT] Message title: Response to message 1
2026/05/12 10:43:51 [CLIENT] Message tags: [gRPC message response]
2026/05/12 10:43:51 [CLIENT] Message data: Response data
2026/05/12 10:43:51 [CLIENT] Message is not last, expecting 4 more messages
2026/05/12 10:43:53 [SERVER] Sending message to stream
2026/05/12 10:43:53 [SERVER] Processing message
2026/05/12 10:43:53 [SERVER] Message 3 of type MESSAGE_TYPE_REQUEST (sent at 2026-05-12 07:43:50.248804093 +0000 UTC)
2026/05/12 10:43:53 [SERVER] Message title: Request message 3
2026/05/12 10:43:53 [SERVER] Message tags: [gRPC message request]
2026/05/12 10:43:53 [SERVER] Message data: Request data
2026/05/12 10:43:53 [SERVER] Message is not last, expecting 2 more messages
2026/05/12 10:43:53 [SERVER] Processing message for 3 seconds
2026/05/12 10:43:53 [CLIENT] Received response message
2026/05/12 10:43:53 [CLIENT] Message 2 of type MESSAGE_TYPE_RESPONSE (sent at 2026-05-12 07:43:53.277063254 +0000 UTC)
2026/05/12 10:43:53 [CLIENT] Message title: Response to message 2
2026/05/12 10:43:53 [CLIENT] Message tags: [gRPC message response]
2026/05/12 10:43:53 [CLIENT] Message data: Response data
2026/05/12 10:43:53 [CLIENT] Message is not last, expecting 3 more messages
2026/05/12 10:43:56 [SERVER] Sending message to stream
2026/05/12 10:43:56 [SERVER] Processing message
2026/05/12 10:43:56 [SERVER] Message 4 of type MESSAGE_TYPE_REQUEST (sent at 2026-05-12 07:43:50.248804234 +0000 UTC)
2026/05/12 10:43:56 [SERVER] Message title: Request message 4
2026/05/12 10:43:56 [SERVER] Message tags: [gRPC message request]
2026/05/12 10:43:56 [SERVER] Message data: Request data
2026/05/12 10:43:56 [SERVER] Message is not last, expecting 1 more messages
2026/05/12 10:43:56 [SERVER] Processing message for 4 seconds
2026/05/12 10:43:56 [CLIENT] Received response message
2026/05/12 10:43:56 [CLIENT] Message 3 of type MESSAGE_TYPE_RESPONSE (sent at 2026-05-12 07:43:56.2793014 +0000 UTC)
2026/05/12 10:43:56 [CLIENT] Message title: Response to message 3
2026/05/12 10:43:56 [CLIENT] Message tags: [gRPC message response]
2026/05/12 10:43:56 [CLIENT] Message data: Response data
2026/05/12 10:43:56 [CLIENT] Message is not last, expecting 2 more messages
2026/05/12 10:44:00 [SERVER] Sending message to stream
2026/05/12 10:44:00 [SERVER] Processing message
2026/05/12 10:44:00 [SERVER] Message 5 of type MESSAGE_TYPE_REQUEST (sent at 2026-05-12 07:43:50.248804436 +0000 UTC)
2026/05/12 10:44:00 [SERVER] Message title: Request message 5
2026/05/12 10:44:00 [SERVER] Message tags: [gRPC message request]
2026/05/12 10:44:00 [SERVER] Message data: Request data
2026/05/12 10:44:00 [SERVER] Message is last, no more messages expected
2026/05/12 10:44:00 [SERVER] Processing message for 5 seconds
2026/05/12 10:44:00 [CLIENT] Received response message
2026/05/12 10:44:00 [CLIENT] Message 4 of type MESSAGE_TYPE_RESPONSE (sent at 2026-05-12 07:44:00.282500532 +0000 UTC)
2026/05/12 10:44:00 [CLIENT] Message title: Response to message 4
2026/05/12 10:44:00 [CLIENT] Message tags: [gRPC message response]
2026/05/12 10:44:00 [CLIENT] Message data: Response data
2026/05/12 10:44:00 [CLIENT] Message is not last, expecting 1 more messages
2026/05/12 10:44:05 [SERVER] Sending message to stream
2026/05/12 10:44:05 [CLIENT] Received response message
2026/05/12 10:44:05 [CLIENT] Message 5 of type MESSAGE_TYPE_RESPONSE (sent at 2026-05-12 07:44:05.286692301 +0000 UTC)
2026/05/12 10:44:05 [CLIENT] Message title: Response to message 5
2026/05/12 10:44:05 [CLIENT] Message tags: [gRPC message response]
2026/05/12 10:44:05 [CLIENT] Message data: Response data
2026/05/12 10:44:05 [CLIENT] Message is last, no more messages expected
```


### How to run examples

You can run all the examples by running the application:

```shell
go run ./cmd/examples/...
```

To choose a certain example you can pass a `-example` flag. For example,

```shell
go run ./cmd/examples/... -example=example3
```

if `-example=all` or the flag is omitted then all the examples will be run. 
