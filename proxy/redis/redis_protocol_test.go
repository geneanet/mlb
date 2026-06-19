package redis

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"strings"
	"testing"
)

// TestRedisProtocolReader_Release verifies that the reader can be released and reused
// via the sync.Pool, and that it correctly resets for a new source.
func TestRedisProtocolReader_Release(t *testing.T) {
	input1 := "+OK\r\n"
	r1 := bytes.NewReader([]byte(input1))
	reader := NewRedisProtocolReader(r1, 128)

	msg1, err := reader.ReadMessage(false)
	if err != nil || !bytes.Equal(msg1, []byte(input1)) {
		t.Fatalf("msg1 failed: %v, %s", err, string(msg1))
	}

	// Capture the bufio.Reader to verify it's the same one later
	br := reader.br
	reader.Release()

	// New reader should potentially pick up the same bufio.Reader from the pool
	input2 := "+PONG\r\n"
	r2 := bytes.NewReader([]byte(input2))
	reader2 := NewRedisProtocolReader(r2, 128)
	defer reader2.Release()

	if reader2.br != br {
		t.Log("Note: sync.Pool did not return the same reader (this is normal but doesn't test reuse)")
	}

	msg2, err := reader2.ReadMessage(false)
	if err != nil || !bytes.Equal(msg2, []byte(input2)) {
		t.Fatalf("msg2 failed: %v, %s", err, string(msg2))
	}
}

// TestReadMessage_SimpleTypes tests parsing of simple, single-line RESP3 types:
// simple strings (+), errors (-), integers (:), doubles (,), big numbers (() nulls (_) and booleans (#).
func TestReadMessage_SimpleTypes(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"SimpleString", "+OK\r\n"},
		{"SimpleError", "-ERR status\r\n"},
		{"Integer", ":42\r\n"},
		{"Double", ",3.14\r\n"},
		{"BigNumber", "(12345678901234567890\r\n"},
		{"Null", "_\r\n"},
		{"BooleanTrue", "#t\r\n"},
		{"BooleanFalse", "#f\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader([]byte(tt.input))
			reader := NewRedisProtocolReader(r, 128)

			msg, err := reader.ReadMessage(false)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !bytes.Equal(msg, []byte(tt.input)) {
				t.Errorf("expected %s, got %s", tt.input, string(msg))
			}
		})
	}
}

// TestReadMessage_DefinedSize tests parsing of fixed-size elements:
// bulk strings ($), errors (!), and verbatim strings (=).
func TestReadMessage_DefinedSize(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"BulkString", "$5\r\nhello\r\n"},
		{"BulkError", "!12\r\nserver_error\r\n"},
		{"VerbatimString", "=13\r\ntxt:some text\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader([]byte(tt.input))
			reader := NewRedisProtocolReader(r, 128)

			msg, err := reader.ReadMessage(false)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !bytes.Equal(msg, []byte(tt.input)) {
				t.Errorf("expected %s, got %s", tt.input, string(msg))
			}
		})
	}
}

// TestReadMessage_StreamedString tests parsing of chunked/streamed strings (e.g. $? followed by ; size chunks).
func TestReadMessage_StreamedString(t *testing.T) {
	input := "$?\r\n;5\r\nhello\r\n;6\r\n world\r\n;0\r\n"
	r := bytes.NewReader([]byte(input))
	reader := NewRedisProtocolReader(r, 128)

	msg, err := reader.ReadMessage(false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !bytes.Equal(msg, []byte(input)) {
		t.Errorf("expected %s, got %s", input, string(msg))
	}
}

// TestReadMessage_Collections tests defined-size arrays (*), sets (~), maps (%), and attributes (|).
func TestReadMessage_Collections(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"Array", "*2\r\n:1\r\n:2\r\n"},
		{"Set", "~2\r\n+foo\r\n+bar\r\n"},
		{"Map", "%1\r\n+key\r\n+value\r\n"},             // Map is 1 key-value pair (2 elements total)
		{"Attribute", "|1\r\n+ttl\r\n:3600\r\n+OK\r\n"}, // Attributes block has 1 key-value pair (2 elements total) followed by a message
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader([]byte(tt.input))
			reader := NewRedisProtocolReader(r, 128)

			msg, err := reader.ReadMessage(false)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !bytes.Equal(msg, []byte(tt.input)) {
				t.Errorf("expected %s, got %s", tt.input, string(msg))
			}
		})
	}
}

// TestReadMessage_StreamedCollections tests streaming collections (e.g., *?, ~?, %?, |?, >?)
// terminated by the end-of-stream dot (.) marker.
func TestReadMessage_StreamedCollections(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"StreamedArray", "*?\r\n:1\r\n:2\r\n:3\r\n.\r\n"},
		{"StreamedAttribute", "|?\r\n+key\r\n:value\r\n.\r\n+OK\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader([]byte(tt.input))
			reader := NewRedisProtocolReader(r, 128)

			msg, err := reader.ReadMessage(false)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !bytes.Equal(msg, []byte(tt.input)) {
				t.Errorf("expected %s, got %s", tt.input, string(msg))
			}
		})
	}
}

// TestReadMessage_InlineCommands verifies inline commands (without type tags)
// are only parsed successfully if allowInline is set to true.
func TestReadMessage_InlineCommands(t *testing.T) {
	input := "PING\r\n"

	t.Run("AllowedInline", func(t *testing.T) {
		r := bytes.NewReader([]byte(input))
		reader := NewRedisProtocolReader(r, 128)

		msg, err := reader.ReadMessage(true)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !bytes.Equal(msg, []byte(input)) {
			t.Errorf("expected %s, got %s", input, string(msg))
		}
	})

	t.Run("DisallowedInline", func(t *testing.T) {
		r := bytes.NewReader([]byte(input))
		reader := NewRedisProtocolReader(r, 128)

		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "RESP3 protocol violation: unsupported item type") {
			t.Errorf("expected error to contain 'RESP3 protocol violation: unsupported item type', got '%s'", err.Error())
		}
	})
}

// TestReadMessage_BufferGrowth verifies that the reader dynamically allocates or grows
// the buffer space when it receives messages larger than the initial buffer configuration.
func TestReadMessage_BufferGrowth(t *testing.T) {
	// Set initial size small enough to trigger grows/moves
	input := "$15\r\nabcdefghijklmno\r\n"
	r := bytes.NewReader([]byte(input))
	reader := NewRedisProtocolReader(r, 4) // tiny buffer

	msg, err := reader.ReadMessage(false)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
	if !bytes.Equal(msg, []byte(input)) {
		t.Errorf("expected %s, got %s", input, string(msg))
	}
}

// TestReadMessage_BufferMove verifies the shift/copy of unparsed bytes to the beginning
// of the buffer when a message start is non-zero, triggering buffer reuse logic.
func TestReadMessage_BufferMove(t *testing.T) {
	input := "+OK\r\n+PONG\r\n"
	r := bytes.NewReader([]byte(input))
	reader := NewRedisProtocolReader(r, 8) // tiny buffer

	// Read first message (+OK\r\n) to shift messageStart
	msg1, err := reader.ReadMessage(false)
	if err != nil {
		t.Errorf("unexpected error reading msg1: %v", err)
	}
	if !bytes.Equal(msg1, []byte("+OK\r\n")) {
		t.Errorf("expected +OK\r\n, got %s", string(msg1))
	}

	// Read second message (+PONG\r\n) which triggers buffer move/reuse
	msg2, err := reader.ReadMessage(false)
	if err != nil {
		t.Errorf("unexpected error reading msg2: %v", err)
	}
	if !bytes.Equal(msg2, []byte("+PONG\r\n")) {
		t.Errorf("expected +PONG\r\n, got %s", string(msg2))
	}
}

// TestReadMessage_SequentialLargeMessages verifies that parsing multiple large messages
// in sequence does not cause issues.
func TestReadMessage_SequentialLargeMessages(t *testing.T) {
	initialSize := 1024

	// Msg 1: Small enough to stay in first grow
	msg1 := []byte("+OK\r\n")

	// Msg 2: Large enough that when buffered it will exceed initialSize
	largeSize := 5000
	largeData := bytes.Repeat([]byte("a"), largeSize)
	msg2 := []byte(fmt.Sprintf("$%d\r\n%s\r\n", largeSize, string(largeData)))

	// We need to ensure msg2 is partially read into the buffer when msg1 is read.
	// bytes.NewReader will allow reading everything at once.
	input := append(msg1, msg2...)
	r := bytes.NewReader(input)
	reader := NewRedisProtocolReader(r, initialSize)

	// Read msg1. This will grow buffer to ~1024 and read msg1 + part of msg2.
	_, err := reader.ReadMessage(false)
	if err != nil {
		t.Fatalf("unexpected error reading msg1: %v", err)
	}

	// Read msg2. It will eventually need to read the rest of the 5000 bytes,
	// triggering readFromSource while messageStart > 0 and unparsed data > 1024.
	_, err = reader.ReadMessage(false)
	if err != nil {
		t.Fatalf("unexpected error reading msg2: %v", err)
	}
}

// errorReader is a helper reader that returns an error after delivering part of the data.
type errorReader struct {
	data  []byte
	index int
	err   error
}

func (e *errorReader) Read(p []byte) (n int, err error) {
	if e.index >= len(e.data) {
		return 0, e.err
	}
	n = copy(p, e.data[e.index:])
	e.index += n
	return n, nil
}

// TestReadMessage_Nesting verifies complex nesting of defined-size and streamed collections.
func TestReadMessage_Nesting(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"NestedStreamInFixedArray", "*2\r\n*?\r\n:1\r\n.\r\n:2\r\n"},
		{"NestedFixedInStreamedArray", "*?\r\n*1\r\n:1\r\n.\r\n"},
		{"BulkStringWithMarker", "$1\r\n*\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := bytes.NewReader([]byte(tt.input))
			reader := NewRedisProtocolReader(r, 128)

			msg, err := reader.ReadMessage(false)
			if err != nil {
				t.Errorf("unexpected error: %v", err)
			}
			if !bytes.Equal(msg, []byte(tt.input)) {
				t.Errorf("expected %s, got %s", tt.input, string(msg))
			}
		})
	}
}

// TestReadMessage_Errors verifies all protocol violations and reading errors.
func TestReadMessage_Errors(t *testing.T) {
	// 1. Unexpected simple type during streamed string
	t.Run("SimpleTypeInStreamedString", func(t *testing.T) {
		r := bytes.NewReader([]byte("$?\r\n+OK\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "RESP3 protocol violation: unexpected item type \"+\" during streamed string") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 2. Unexpected defined size type during streamed string
	t.Run("DefinedSizeTypeInStreamedString", func(t *testing.T) {
		r := bytes.NewReader([]byte("$?\r\n$5\r\nhello\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "RESP3 protocol violation: unexpected item type \"$\" during streamed string") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 3. Unexpected streamed string chunk outside streamed string
	t.Run("UnexpectedStreamedChunk", func(t *testing.T) {
		r := bytes.NewReader([]byte(";5\r\nhello\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "RESP3 protocol violation: unexpected streamed string") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 4. Unexpected collection start during streamed string
	t.Run("CollectionStartInStreamedString", func(t *testing.T) {
		r := bytes.NewReader([]byte("$?\r\n*2\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "RESP3 protocol violation: unexpected item type \"*\" during streamed string") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 5. Unexpected dot (.) marker while not in a streamed collection
	t.Run("UnexpectedDotMarker", func(t *testing.T) {
		r := bytes.NewReader([]byte(".\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "RESP3 protocol violation: unexpected item . while not streaming") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 6. Dot (.) marker during streamed string
	t.Run("DotMarkerInStreamedString", func(t *testing.T) {
		r := bytes.NewReader([]byte("$?\r\n.\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "RESP3 protocol violation: unexpected item type \".\" during streamed string") {
			t.Errorf("unexpected error: %v", err)
		}
	})

	// 7. Invalid size parsing in defined size type
	t.Run("InvalidSizeParseDefined", func(t *testing.T) {
		r := bytes.NewReader([]byte("$ABC\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	// 8. Invalid size parsing in streamed chunk
	t.Run("InvalidSizeParseChunk", func(t *testing.T) {
		r := bytes.NewReader([]byte("$?\r\n;ABC\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	// 9. Invalid size parsing in collections
	t.Run("InvalidSizeParseCollection", func(t *testing.T) {
		r := bytes.NewReader([]byte("*ABC\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil {
			t.Errorf("expected error, got nil")
		}
	})

	// 10. IO read error from source (empty read)
	t.Run("IOErrorFromSource", func(t *testing.T) {
		expectedErr := errors.New("simulated read error")
		er := &errorReader{
			data: []byte("+OK"), // incomplete line
			err:  expectedErr,
		}
		reader := NewRedisProtocolReader(er, 128)
		_, err := reader.ReadMessage(false)
		if !errors.Is(err, expectedErr) {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	// 11. IO read error from source with non-zero bytes read
	t.Run("IOErrorWithBytes", func(t *testing.T) {
		expectedErr := errors.New("simulated read error with bytes")
		// We set data and error such that we read "+OK" but hit error on the same call
		// Let's implement a specific reader function for this subtest
		type customReader struct {
			err error
		}
		cr := &customReader{err: expectedErr}
		readerFn := func(p []byte) (n int, err error) {
			n = copy(p, []byte("+OK"))
			return n, cr.err
		}

		type fnReader struct {
			read func([]byte) (int, error)
		}
		fr := &fnReader{read: readerFn}

		reader := NewRedisProtocolReader(interfaceReader{fr.read}, 128)
		_, err := reader.ReadMessage(false)
		if !errors.Is(err, expectedErr) {
			t.Errorf("expected error %v, got %v", expectedErr, err)
		}
	})

	// 12. EOF with empty input
	t.Run("EOFEmptyInput", func(t *testing.T) {
		r := bytes.NewReader([]byte{})
		reader := NewRedisProtocolReader(r, 128)
		msg, err := reader.ReadMessage(false)
		if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
		if len(msg) != 0 {
			t.Errorf("expected empty message, got %v", msg)
		}
	})

	// 13. EOF with incomplete line
	t.Run("EOFIncompleteLine", func(t *testing.T) {
		r := bytes.NewReader([]byte("+OK"))
		reader := NewRedisProtocolReader(r, 128)
		msg, err := reader.ReadMessage(false)
		if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
		if !bytes.Equal(msg, []byte("+OK")) {
			t.Errorf("expected +OK, got %s", string(msg))
		}
	})

	// 14. Bulk string with internal CRLF (triggers minimumBytes check in readLine)
	t.Run("BulkStringWithInternalCRLF", func(t *testing.T) {
		input := "$5\r\na\r\nbc\r\n"
		r := bytes.NewReader([]byte(input))
		reader := NewRedisProtocolReader(r, 128)
		msg, err := reader.ReadMessage(false)
		if err != nil {
			t.Errorf("unexpected error: %v", err)
		}
		if !bytes.Equal(msg, []byte(input)) {
			t.Errorf("expected %s, got %s", input, string(msg))
		}
	})

	// 15. Malformed short message at EOF (should not panic)
	t.Run("MalformedShortAtEOF", func(t *testing.T) {
		input := "*"
		r := bytes.NewReader([]byte(input))
		reader := NewRedisProtocolReader(r, 128)
		msg, err := reader.ReadMessage(false)
		if !errors.Is(err, io.EOF) {
			t.Errorf("expected io.EOF, got %v", err)
		}
		if !bytes.Equal(msg, []byte(input)) {
			t.Errorf("expected %s, got %s", input, string(msg))
		}
	})

	// 16. Various RESP types for coverage
	t.Run("AdditionalTypesCoverage", func(t *testing.T) {
		tests := []struct {
			name  string
			input string
		}{
			{"NullBulkString", "$-1\r\n"},
			{"EmptyArray", "*0\r\n"},
			{"PushMessage", ">2\r\n+pubsub\r\n+message\r\n"},
			{"Map", "%1\r\n+key\r\n:1\r\n"},
			{"Set", "~1\r\n:1\r\n"},
			{"Verbatim", "=7\r\ntxt:abc\r\n"},
			{"BulkError", "!3\r\nerr\r\n"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				r := bytes.NewReader([]byte(tt.input))
				reader := NewRedisProtocolReader(r, 128)
				msg, err := reader.ReadMessage(false)
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				}
				if !bytes.Equal(msg, []byte(tt.input)) {
					t.Errorf("expected %s, got %s", tt.input, string(msg))
				}
			})
		}
	})

	// 17. EOF during various headers for coverage
	t.Run("EOFInHeadersCoverage", func(t *testing.T) {
		tests := []struct {
			name  string
			input string
		}{
			{"ShortBulkHeader", "$1"},
			{"ShortChunkHeader", "$?\r\n;1"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				r := bytes.NewReader([]byte(tt.input))
				reader := NewRedisProtocolReader(r, 128)
				msg, err := reader.ReadMessage(false)
				if !errors.Is(err, io.EOF) {
					t.Errorf("expected io.EOF, got %v", err)
				}
				if !bytes.Equal(msg, []byte(tt.input)) {
					t.Errorf("expected %s, got %s", tt.input, string(msg))
				}
			})
		}
	})
	// 18. Protocol violations for 100% coverage
	t.Run("ProtocolViolationsCoverage", func(t *testing.T) {
		tests := []struct {
			name  string
			input string
			err   string
		}{
			{"InvalidBulkSize", "$abc\r\n", "invalid integer"},
			{"InvalidChunkSize", "$?\r\n;abc\r\n", "invalid integer"},
			{"InvalidCollectionSize", "*abc\r\n", "invalid integer"},
			{"UnexpectedStreamedString", ";5\r\nhello\r\n", "unexpected streamed string"},
			{"StreamedStringViolation", "$?\r\n+OK\r\n", "unexpected item type"},
			{"StreamedStringViolationBulk", "$?\r\n$5\r\nhello\r\n", "unexpected item type"},
			{"StreamedStringViolationColl", "$?\r\n*1\r\n:1\r\n", "unexpected item type"},
			{"StreamedStringViolationDot", "$?\r\n.\r\n", "unexpected item type"},
			{"UnexpectedDot", ".\r\n", "while not streaming"},
		}
		for _, tt := range tests {
			t.Run(tt.name, func(t *testing.T) {
				r := bytes.NewReader([]byte(tt.input))
				reader := NewRedisProtocolReader(r, 128)
				_, err := reader.ReadMessage(false)
				if err == nil {
					t.Errorf("expected error, got nil")
				}
			})
		}
	})

	// 19. readLine error case
	t.Run("ReadLineError", func(t *testing.T) {
		expectedErr := errors.New("read fail")
		er := &errorReader{
			data: []byte("+"),
			err:  expectedErr,
		}
		reader := NewRedisProtocolReader(er, 128)
		_, err := reader.ReadMessage(false)
		if !errors.Is(err, expectedErr) {
			t.Errorf("expected %v, got %v", expectedErr, err)
		}
	})

	// 20. coverage for inline error and empty line
	t.Run("InlineErrorAndEmpty", func(t *testing.T) {
		r := bytes.NewReader([]byte("PING\r\n"))
		reader := NewRedisProtocolReader(r, 128)
		_, err := reader.ReadMessage(false)
		if err == nil || !strings.Contains(err.Error(), "unsupported item type") {
			t.Errorf("expected unsupported item type error, got %v", err)
		}
	})
}

// TestParseSize verifies the decimal integer parsing logic for RESP sizes.
func TestParseSize(t *testing.T) {
	tests := []struct {
		input   string
		want    int
		wantErr bool
	}{
		{"42", 42, false},
		{"0", 0, false},
		{"-1", -1, false},
		{"-42", -42, false},
		{"", 0, true},
		{"-", 0, true},
		{"abc", 0, true},
		{"12a", 0, true},
		{"-12a", 0, true},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			got, err := parseSize([]byte(tt.input))
			if (err != nil) != tt.wantErr {
				t.Errorf("parseSize() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("parseSize() got = %v, want %v", got, tt.want)
			}
		})
	}
}

// interfaceReader is a helper to wrap a read function in an io.Reader.

type interfaceReader struct {
	read func([]byte) (int, error)
}

func (ir interfaceReader) Read(p []byte) (int, error) {
	return ir.read(p)
}
