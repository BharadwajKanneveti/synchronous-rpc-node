package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

// Message is one JSON message; Body holds its fields.
type Message struct {
	Src  string         `json:"src"`
	Dest string         `json:"dest"`
	Body map[string]any `json:"body"`
}

const maxLineBytes = 16 << 20

var (
	out         = bufio.NewWriter(os.Stdout)
	outMutex    sync.Mutex
	nodeID      string
	nodeIDs     []string
	nextMsgID   int64
	pendingRPCs sync.Map
)

// emit writes a payload safely across concurrent goroutines using outMutex
func emit(value any) {
	data, err := json.Marshal(value)
	if err != nil {
		fmt.Fprintln(os.Stderr, "cannot encode:", err)
		return
	}

	outMutex.Lock()
	defer outMutex.Unlock()
	out.Write(data)
	out.WriteString("\n")
	out.Flush()
}

// send sends a message to another node or client with an atomically generated unique msg_id.
func send(dest string, body map[string]any) {
	body["msg_id"] = atomic.AddInt64(&nextMsgID, 1)
	emit(Message{Src: nodeID, Dest: dest, Body: body})
}

// reply answers a request using the specification's decrementing msg_id rule (reqMsgID - 1)
func reply(request Message, body map[string]any) {
	reqMsgID, _ := request.Body["msg_id"].(float64)
	body["in_reply_to"] = request.Body["msg_id"]
	body["msg_id"] = int(reqMsgID) - 1
	emit(Message{Src: nodeID, Dest: request.Src, Body: body})
}

// syncRPC forwards an inner payload to target, waiting up to timeout for a reply
func syncRPC(target string, inner map[string]any, timeout time.Duration) (map[string]any, error) {
	outMsgID := atomic.AddInt64(&nextMsgID, 1)

	reqBody := make(map[string]any)
	for k, v := range inner {
		reqBody[k] = v
	}
	reqBody["msg_id"] = outMsgID

	ch := make(chan map[string]any, 1)
	pendingRPCs.Store(outMsgID, ch)
	defer pendingRPCs.Delete(outMsgID)

	emit(Message{Src: nodeID, Dest: target, Body: reqBody})

	select {
	case replyBody := <-ch:
		return replyBody, nil
	case <-time.After(timeout):
		return nil, fmt.Errorf("RPC timeout")
	}
}

func handle_init(message Message) {
	if id, ok := message.Body["node_id"].(string); ok {
		nodeID = id
	}
	if ids, ok := message.Body["node_ids"].([]any); ok {
		nodeIDs = make([]string, len(ids))
		for i, v := range ids {
			nodeIDs[i], _ = v.(string)
		}
	}
	reply(message, map[string]any{"type": "init_ok"})
}

func handle_echo(message Message) {
	reply(message, map[string]any{
		"type": "echo_ok",
		"echo": message.Body["echo"],
	})
}

func handle_proxy(message Message) {
	target, _ := message.Body["target"].(string)
	inner, _ := message.Body["inner"].(map[string]any)

	// Pre-register ID and channel synchronously before spawning the goroutine
	// to prevent local file race conditions.
	outMsgID := atomic.AddInt64(&nextMsgID, 1)
	ch := make(chan map[string]any, 1)
	pendingRPCs.Store(outMsgID, ch)

	// Offload to goroutine so main reader loop on stdin remains unblocked
	go func(req Message, msgID int64, replyChan chan map[string]any) {
		defer pendingRPCs.Delete(msgID)

		reqBody := make(map[string]any)
		for k, v := range inner {
			reqBody[k] = v
		}
		reqBody["msg_id"] = msgID

		emit(Message{Src: nodeID, Dest: target, Body: reqBody})

		select {
		case replyBody := <-replyChan:
			reply(req, map[string]any{
				"type":   "proxy_ok",
				"result": replyBody,
			})
		case <-time.After(9000 * time.Millisecond):
			reply(req, map[string]any{
				"type": "error",
				"code": 0,
				"text": "RPC timeout",
			})
		}
	}(message, outMsgID, ch)
}

var handlers = map[string]func(Message){
	"init":  handle_init,
	"echo":  handle_echo,
	"proxy": handle_proxy,
}

func main() {
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Buffer(make([]byte, maxLineBytes), maxLineBytes)

	for scanner.Scan() {
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		var message Message
		if err := json.Unmarshal(line, &message); err != nil {
			fmt.Fprintln(os.Stderr, "Skipping malformed line:", string(line))
			continue
		}

		// Check if this incoming line is a response to an active syncRPC call
		if inReplyTo, ok := message.Body["in_reply_to"].(float64); ok {
			if ch, loaded := pendingRPCs.Load(int64(inReplyTo)); loaded {
				ch.(chan map[string]any) <- message.Body
				continue
			}
		}

		// Dispatch request to registered handler
		msgType, _ := message.Body["type"].(string)
		if handler, ok := handlers[msgType]; ok {
			handler(message)
		}
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintln(os.Stderr, "error reading standard input:", err)
	}

	// Brief delay for local testing so background goroutines can finish writing output
	time.Sleep(100 * time.Millisecond)
}
