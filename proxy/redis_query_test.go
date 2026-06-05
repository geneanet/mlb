package proxy

import (
	"bytes"
	"strings"
	"testing"
)

// TestNewRedisQuery verifies that NewRedisQuery correctly instantiates a RedisQuery object,
// assigns a unique auto-incrementing ID, and properly links the data and channel parameters.
func TestNewRedisQuery(t *testing.T) {
	// Setup channels
	responseChan := make(chan RedisReponse, 1)
	responseChanStop := make(chan struct{})
	defer close(responseChanStop)

	// Create a query
	item := []byte("PING\r\n")
	query := NewRedisQuery(item, responseChan, responseChanStop)

	// Verify assignments
	if query.id <= 0 {
		t.Errorf("Query ID should be greater than 0, got %d", query.id)
	}
	if !bytes.Equal(item, query.item) {
		t.Errorf("Query item should match input, expected %v, got %v", item, query.item)
	}
	if responseChan != query.responseChan {
		t.Errorf("Response channel should match input")
	}
	if responseChanStop != query.responseChanStop {
		t.Errorf("Response stop channel should match input")
	}
}

// TestRedisQuery_Reply verifies that Reply successfully sends the response to the client
// response channel, and returns an error if the response stop channel is closed.
func TestRedisQuery_Reply(t *testing.T) {
	// Case 1: Normal reply delivery
	t.Run("NormalReply", func(t *testing.T) {
		responseChan := make(chan RedisReponse, 1)
		responseChanStop := make(chan struct{})

		query := NewRedisQuery([]byte("PING\r\n"), responseChan, responseChanStop)
		replyData := []byte("+PONG\r\n")
		err := query.Reply(replyData)

		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		select {
		case resp := <-responseChan:
			if query.id != resp.query.id {
				t.Errorf("expected ID %d, got %d", query.id, resp.query.id)
			}
			if !bytes.Equal(replyData, resp.item) {
				t.Errorf("expected %v, got %v", replyData, resp.item)
			}
		default:
			t.Fatal("Expected response to be on responseChan")
		}
	})

	// Case 2: Reply fails when client connection / response is stopped/closed
	t.Run("StoppedReply", func(t *testing.T) {
		responseChan := make(chan RedisReponse) // Unbuffered
		responseChanStop := make(chan struct{})

		query := NewRedisQuery([]byte("PING\r\n"), responseChan, responseChanStop)
		close(responseChanStop) // Simulate stopped client connection

		replyData := []byte("+PONG\r\n")
		err := query.Reply(replyData)

		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if err != nil && !strings.Contains(err.Error(), "response channel is closed") {
			t.Errorf("expected error to contain 'response channel is closed', got '%s'", err.Error())
		}
	})
}

// TestRedisQuery_Abort verifies that Abort sends a nil reply to signal that the query execution
// has been cancelled or the backend connection aborted.
func TestRedisQuery_Abort(t *testing.T) {
	responseChan := make(chan RedisReponse, 1)
	responseChanStop := make(chan struct{})

	query := NewRedisQuery([]byte("PING\r\n"), responseChan, responseChanStop)
	err := query.Abort()

	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	select {
	case resp := <-responseChan:
		if resp.item != nil {
			t.Errorf("Aborted query response item should be nil, got %v", resp.item)
		}
	default:
		t.Fatal("Expected response to be on responseChan")
	}
}

// TestRedisQuery_GetCommand verifies the command extraction logic for both inline text queries
// and RESP3 array formats, as well as testing syntax validation and formatting errors.
func TestRedisQuery_GetCommand(t *testing.T) {
	// Subtest 1: Short invalid command
	t.Run("InvalidCommandLength", func(t *testing.T) {
		query := NewRedisQuery([]byte("PI"), nil, nil)
		cmd, err := query.GetCommand()
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if len(cmd) != 0 {
			t.Errorf("expected empty command, got %v", cmd)
		}
		if err != nil && err.Error() != "invalid command" {
			t.Errorf("expected error 'invalid command', got '%v'", err.Error())
		}
	})

	// Subtest 2: Inline command without spaces
	t.Run("InlineWithoutSpaces", func(t *testing.T) {
		query := NewRedisQuery([]byte("PING\r\n"), nil, nil)
		cmd, err := query.GetCommand()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !bytes.Equal(cmd, []byte("PING")) {
			t.Errorf("expected PING, got %s", string(cmd))
		}
	})

	// Subtest 3: Inline command with spaces (arguments)
	t.Run("InlineWithSpaces", func(t *testing.T) {
		query := NewRedisQuery([]byte("GET key_name\r\n"), nil, nil)
		cmd, err := query.GetCommand()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !bytes.Equal(cmd, []byte("GET")) {
			t.Errorf("expected GET, got %s", string(cmd))
		}
	})

	// Subtest 4: Array command (starts with '*') - successful parse
	// Format: *<number_of_elements>\r\n$<length_of_first_element>\r\n<first_element>\r\n...
	t.Run("ArrayValid", func(t *testing.T) {
		query := NewRedisQuery([]byte("*2\r\n$4\r\nPING\r\n$3\r\nfoo\r\n"), nil, nil)
		cmd, err := query.GetCommand()
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !bytes.Equal(cmd, []byte("PING")) {
			t.Errorf("expected PING, got %s", string(cmd))
		}
	})

	// Subtest 5: Array command - missing bulk string indicator '$'
	t.Run("ArrayMissingBulkIndicator", func(t *testing.T) {
		query := NewRedisQuery([]byte("*2\r\nPING\r\n"), nil, nil)
		cmd, err := query.GetCommand()
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if len(cmd) != 0 {
			t.Errorf("expected empty command, got %v", cmd)
		}
		if err != nil && !strings.Contains(err.Error(), "bulk string start not found") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// Subtest 6: Array command - missing carriage return '\r' after '$' size
	t.Run("ArrayMissingCarriageReturn", func(t *testing.T) {
		query := NewRedisQuery([]byte("*2\r\n$4"), nil, nil)
		cmd, err := query.GetCommand()
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if len(cmd) != 0 {
			t.Errorf("expected empty command, got %v", cmd)
		}
		if err != nil && !strings.Contains(err.Error(), "bulk string end not found") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// Subtest 7: Array command - invalid integer bulk size
	t.Run("ArrayInvalidBulkSize", func(t *testing.T) {
		query := NewRedisQuery([]byte("*2\r\n$XYZ\r\nPING\r\n"), nil, nil)
		cmd, err := query.GetCommand()
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if len(cmd) != 0 {
			t.Errorf("expected empty command, got %v", cmd)
		}
		if err != nil && !strings.Contains(err.Error(), "unable to parse bulk string size") {
			t.Errorf("unexpected error: %v", err)
		}
	})
}

// TestRedisQuery_IsRestricted verifies that restricted and dangerous Redis commands
// are correctly identified, while normal commands are permitted.
func TestRedisQuery_IsRestricted(t *testing.T) {
	// Case 1: Permitted commands
	allowedCommands := []string{"GET", "SET", "PING", "DEL", "EXISTS"}
	for _, cmdStr := range allowedCommands {
		t.Run("Allowed_"+cmdStr, func(t *testing.T) {
			query := NewRedisQuery([]byte(cmdStr+"\r\n"), nil, nil)
			if query.IsRestricted() {
				t.Errorf("Command %s should be allowed", cmdStr)
			}
		})
	}

	// Case 2: Restricted transactions/multi commands
	restricted := []string{
		"watch", "unwatch", "multi", "exec", "discard",
		"brpoplpush", "blpop", "brpop", "bzpopmin", "bzpopmax", "xread", "xreadgroup", "wait", "waitaof",
		"subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ssubscribe", "sunsubscribe", "publish", "spublish", "pubsub",
		"monitor",
	}
	for _, cmdStr := range restricted {
		t.Run("Restricted_Lower_"+cmdStr, func(t *testing.T) {
			query := NewRedisQuery([]byte(cmdStr+"\r\n"), nil, nil)
			if !query.IsRestricted() {
				t.Errorf("Command %s should be denied", cmdStr)
			}
		})
		t.Run("Restricted_Upper_"+cmdStr, func(t *testing.T) {
			query := NewRedisQuery([]byte(strings.ToUpper(cmdStr)+"\r\n"), nil, nil)
			if !query.IsRestricted() {
				t.Errorf("Command %s should be denied case-insensitively", cmdStr)
			}
		})
	}

	// Case 3: Invalid commands that fail parsing should also not be allowed
	t.Run("InvalidCommandDenied", func(t *testing.T) {
		query := NewRedisQuery([]byte("PI"), nil, nil)
		if !query.IsRestricted() {
			t.Errorf("Invalid command should be disallowed")
		}
	})
}
