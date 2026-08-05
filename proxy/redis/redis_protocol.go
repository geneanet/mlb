package redis

import (
	"bufio"
	"fmt"
	"io"
	"mlb/util"
	"slices"
	"sync"
)

//-----------------
// Protocol parsing
//-----------------

// readerPool allows reuse of bufio.Reader instances to minimize memory allocations
// during high-concurrency Redis proxying.
var readerPool sync.Pool

// bufferPool allows reuse of byte slices to minimize allocations during ReadMessage.
var bufferPool = sync.Pool{
	New: func() any {
		return make([]byte, 0, 4096)
	},
}

// ReleaseBuffer returns a buffer to the pool.
func ReleaseBuffer(b []byte) {
	if b == nil || cap(b) == 0 {
		return
	}
	bufferPool.Put(b)
}

// RedisProtocolReader provides a high-level reader for Redis RESP2 and RESP3 protocols.
// It uses an internal bufio.Reader for efficiency and maintains an accumulation buffer
// to return complete protocol messages.
type RedisProtocolReader struct {
	br     *bufio.Reader
	buffer []byte // Accumulation buffer for the current message
}

// NewRedisProtocolReader creates a new reader, potentially reusing a bufio.Reader from the pool.
// initialBufferSize specifies the starting capacity of the internal bufio.Reader if one is created.
func NewRedisProtocolReader(reader io.Reader, bufferSize int) *RedisProtocolReader {
	var br *bufio.Reader
	if v := readerPool.Get(); v != nil {
		br = v.(*bufio.Reader)
		br.Reset(reader)
	} else {
		br = bufio.NewReaderSize(reader, bufferSize)
	}
	return &RedisProtocolReader{
		br:     br,
		buffer: nil,
	}
}

// Release returns the internal bufio.Reader and any remaining accumulation buffer to their respective pools.
// It MUST be called once the reader is no longer needed to ensure memory efficiency.
func (r *RedisProtocolReader) Release() {
	if r.br != nil {
		r.br.Reset(nil)
		readerPool.Put(r.br)
		r.br = nil
	}
	if r.buffer != nil {
		ReleaseBuffer(r.buffer)
		r.buffer = nil
	}
}

// readLine reads a single CRLF-terminated line from the source and appends it to the accumulation buffer.
func (r *RedisProtocolReader) readLine() ([]byte, error) {
	if r.buffer == nil {
		r.buffer = bufferPool.Get().([]byte)
		r.buffer = r.buffer[:0]
	}

	start := len(r.buffer)
	var err error
	for {
		var line []byte
		line, err = r.br.ReadSlice('\n')
		r.buffer = append(r.buffer, line...)
		if err != bufio.ErrBufferFull {
			break
		}
	}
	return r.buffer[start:], err
}

// readRaw reads exactly n+2 bytes (payload + CRLF) from the source and appends them to the accumulation buffer.
func (r *RedisProtocolReader) readRaw(n int) ([]byte, error) {
	if r.buffer == nil {
		r.buffer = bufferPool.Get().([]byte)
		r.buffer = r.buffer[:0]
	}

	// Bulk types have a trailing CRLF (\r\n)
	total := n + 2
	start := len(r.buffer)
	r.buffer = slices.Grow(r.buffer, total)
	r.buffer = r.buffer[:start+total]
	nread, err := io.ReadFull(r.br, r.buffer[start:])
	r.buffer = r.buffer[:start+nread]
	return r.buffer[start:], err
}

// ReadMessage parses a single complete RESP message from the source reader.
// It handles simple types, bulk strings, and nested collections (Arrays, Maps, Sets, etc.).
// If allowInline is true, it also supports simple space-separated inline commands.
func (r *RedisProtocolReader) ReadMessage(allowInline bool) ([]byte, error) {
	if r.buffer == nil {
		r.buffer = bufferPool.Get().([]byte)
	}
	r.buffer = r.buffer[:0]
	eof := false

	// stack tracks the nesting levels and expected items remaining at each level.
	// Positive (n > 0): Number of items remaining in a fixed-size collection.
	// -1: Currently parsing a streamed collection (waiting for '.' terminator).
	// -2: Currently parsing a streamed bulk string (waiting for ';' terminator).
	stack := make([]int, 0, 8)
	stack = append(stack, 1) // Expect one top-level message

	for len(stack) > 0 {
		line, err := r.readLine()
		if err != nil {
			if err == io.EOF {
				eof = true
				if len(line) == 0 {
					break
				}
			} else {
				return nil, err
			}
		}

		topIdx := len(stack) - 1
		inStreamedString := stack[topIdx] == -2

		// Decrement remaining items if we are in a fixed collection.
		if stack[topIdx] > 0 {
			stack[topIdx]--
		}

		switch line[0] {
		case '+', '-', ':', '_', ',', '#', '(':
			// Simple types: String (+), Error (-), Integer (:), Null (_), Double (,), Boolean (#), BigNumber (()
			if inStreamedString {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

		case '$', '!', '=':
			// Bulk types: Bulk String ($), Bulk Error (!), Verbatim String (=)
			if inStreamedString {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

			// Streamed bulk string start: $?\r\n
			if len(line) == 4 && line[0] == '$' && line[1] == '?' {
				stack = append(stack, -2)
			} else {
				// RESP Bulk headers are $[size]\r\n, so minimum is 4 bytes (e.g. $0\r\n)
				// RESP2 Null Bulk String is $-1\r\n (5 bytes)
				if len(line) < 4 {
					if eof {
						break
					}
					return nil, fmt.Errorf("RESP3 protocol violation: malformed bulk header")
				}
				size, errAtoi := util.ParseSize(line[1 : len(line)-2])
				if errAtoi != nil {
					if eof {
						break
					}
					return nil, errAtoi
				}
				// Null bulk strings ($-1) have no data follow-up
				if size >= 0 {
					_, err := r.readRaw(size)
					if err != nil {
						return nil, err
					}
				}
			}

		case ';':
			// Streamed string chunk (;size\r\ndata\r\n)
			if !inStreamedString {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected streamed string chunk")
			}
			if len(line) < 4 {
				if eof {
					break
				}
				return nil, fmt.Errorf("RESP3 protocol violation: malformed streamed string chunk header")
			}
			size, errAtoi := util.ParseSize(line[1 : len(line)-2])
			if errAtoi != nil {
				if eof {
					break
				}
				return nil, errAtoi
			}
			if size > 0 {
				_, err := r.readRaw(size)
				if err != nil {
					return nil, err
				}
			} else {
				// Final chunk (;0\r\n), pop streamed string state
				stack = stack[:len(stack)-1]
			}

		case '*', '~', '%', '|', '>':
			// Collection types: Array (*), Set (~), Map (%), Attribute (|), Push (>)
			if inStreamedString {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

			// Streamed collection start (e.g., *?\r\n). Note: Push (>) cannot be streamed.
			if len(line) == 4 && line[0] != '>' && line[1] == '?' {
				if line[0] == '|' {
					stack = append(stack, 1) // Attributes prefix: one more message after stream
				}
				stack = append(stack, -1)
			} else {
				// Collection headers are *[size]\r\n, so minimum is 4 bytes (e.g. *0\r\n)
				// RESP2 Null Array is *-1\r\n (5 bytes)
				if len(line) < 4 {
					if eof {
						break
					}
					return nil, fmt.Errorf("RESP3 protocol violation: malformed collection header")
				}
				size, errAtoi := util.ParseSize(line[1 : len(line)-2])
				if errAtoi != nil {
					if eof {
						break
					}
					return nil, errAtoi
				}

				count := size
				// Hashes (%) and Attributes (|) are followed by pairs of items
				if line[0] == '%' || line[0] == '|' {
					if count > 0 {
						count *= 2
					}
				}
				// Attributes (|) are prefixes; we must read one more complete message after the pairs/stream
				if line[0] == '|' {
					count++
				}

				// Null collections (e.g. *-1\r\n) or empty ones (*0\r\n) have no items following
				if count > 0 {
					stack = append(stack, count)
				}
			}

		case '.':
			// Streamed collection terminator (.\r\n)
			if inStreamedString {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \".\" during streamed string")
			}
			if stack[topIdx] != -1 {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item . while not streaming")
			}
			stack = stack[:len(stack)-1]

		default:
			// Inline commands or unknown protocol markers
			if !allowInline {
				return nil, fmt.Errorf("RESP3 protocol violation: unsupported item type \"%s\"", string(line[0]))
			}
		}

		// Pop finished levels (remaining count == 0)
		for len(stack) > 0 && stack[len(stack)-1] == 0 {
			stack = stack[:len(stack)-1]
		}
		allowInline = false
	}

	if eof && len(r.buffer) == 0 {
		return nil, io.EOF
	}

	res := r.buffer
	r.buffer = nil

	var err error
	if eof {
		err = io.EOF
	}
	return res, err
}
