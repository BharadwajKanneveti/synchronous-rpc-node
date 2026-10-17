
# Synchronous RPC Proxy in Go

A simple Go project to understand how nodes communicate using JSON messages and synchronous RPC.

### Features
- Initialize a node and handle echo requests.
- Forward requests to other nodes and wait for responses.
- Use goroutines, channels, and timeouts to handle concurrent requests.

### How to Run
```bash
go run main.go
```

**Note:** Provide JSON inputs one line at a time through standard input to get outputs. The program processes inputs as they arrive, so send each message promptly to observe the request-response flow.
