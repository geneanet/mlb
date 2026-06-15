package proxy

import (
	"fmt"
	"io"
	"slices"
)

//-----------------
// Protocol parsing
//-----------------

// TODO: improve perfs by using buffer pools

// parseSize parses a decimal integer from a byte slice without performing heap allocations.
// It supports an optional leading '-' for negative numbers and validates that all characters
// are valid decimal digits. It returns the parsed integer or an error if the input is empty
// or contains invalid characters.
func parseSize(b []byte) (int, error) {
	if len(b) == 0 {
		return 0, fmt.Errorf("empty integer")
	}

	neg := false
	if b[0] == '-' {
		neg = true
		b = b[1:]
		if len(b) == 0 {
			return 0, fmt.Errorf("invalid integer: \"-\"")
		}
	}

	res := 0
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, fmt.Errorf("invalid integer")
		}
		res = res*10 + int(c-'0')
	}

	if neg {
		return -res, nil
	}
	return res, nil
}

// RedisProtocolReader provides a high-level reader for Redis RESP2 and RESP3 protocols.
// It handles buffered reading and automatic buffer management to support messages of any size.
type RedisProtocolReader struct {
	reader            io.Reader // Source of raw data
	buffer            []byte    // Internal buffer for reading and parsing
	readPosition      int       // Current offset in the buffer during parsing
	lineStart         int       // Start offset of the current line being parsed
	messageStart      int       // Start offset of the entire message currently being read
	initialBufferSize int       // Default size for buffer allocations
	minimumReadSize   int       // Minimum free space required before performing a new read from source
}

// NewRedisProtocolReader creates a new reader with a specified initial buffer size.
func NewRedisProtocolReader(reader io.Reader, initialBufferSize int) RedisProtocolReader {
	return RedisProtocolReader{
		reader:            reader,
		initialBufferSize: initialBufferSize,
		minimumReadSize:   64,
	}
}

// ReadMessage parses a single complete RESP message from the source reader.
// It supports both RESP2 and RESP3, including complex types like streamed strings,
// collections (arrays, maps, sets), and attributes.
//
// Parameters:
//   - allowInline: If true, allows parsing simple inline commands (e.g., "PING\r\n").
//
// Returns the raw bytes of the entire message, including protocol markers and CRLF.
func (r *RedisProtocolReader) ReadMessage(allowInline bool) ([]byte, error) {
	r.messageStart = r.readPosition
	eof := false

	// raw is true when the next line to read is an unformatted bulk payload (fixed-size).
	// In this state, we skip RESP marker detection to avoid misinterpreting binary data.
	raw := false

	// bytesToRead is the expected size of a bulk payload. It is passed to readLine to
	// ensure internal CRLFs within the payload don't prematurely end the line.
	bytesToRead := 0

	// stack tracks the nesting levels and expected items remaining at each level.
	// Positive (n > 0): Number of items remaining in a fixed-size collection (Array, Map, etc.)
	//                   or 1 for a pending raw bulk payload line.
	// -1: Currently parsing a streamed collection (waiting for '.' terminator).
	// -2: Currently parsing a streamed bulk string (waiting for ';0' terminator).
	stack := make([]int, 0, 8)
	stack = append(stack, 1) // Expect one top-level message initially

	for len(stack) > 0 && !eof {
		line, err := r.readLine(bytesToRead)
		bytesToRead = 0 // Reset after use; next line might be a header or a simple type
		if err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			eof = true
		}

		if len(line) == 0 {
			continue
		}

		topIdx := len(stack) - 1
		inStreamedString := stack[topIdx] == -2

		// Decrement remaining items if we are in a fixed collection or reading raw data.
		// A value of 1 for a raw line will reach 0 and trigger an automatic pop below.
		if stack[topIdx] > 0 {
			stack[topIdx]--
		}

		if raw {
			raw = false
		} else {
			// Parse the line type based on the first character (RESP marker)
			switch line[0] {
			case '+', '-', ':', '_', ',', '#', '(':
				// Simple types: String, Error, Integer, Null, Double, Boolean, BigNumber
				if inStreamedString {
					return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
				}

			case '$', '!', '=':
				// Fixed-size bulk types: Bulk String, Bulk Error, Verbatim String
				if inStreamedString {
					return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
				}

				if len(line) >= 2 && line[0] == '$' && line[1] == '?' { // Streamed string start ($?\r\n)
					stack = append(stack, -2)
				} else { // Defined size (e.g., $12\r\n)
					if len(line) < 4 {
						if eof {
							break
						}
						return nil, fmt.Errorf("RESP3 protocol violation: malformed bulk string header")
					}
					size, errAtoi := parseSize(line[1 : len(line)-2])
					if errAtoi != nil {
						if eof {
							break
						}
						return nil, errAtoi
					}

					if size >= 0 {
						// Expect one raw data line
						stack = append(stack, 1)
						bytesToRead = size
						raw = true
					}
				}

			case ';':
				// Streamed string chunk (;size\r\ndata\r\n)
				if !inStreamedString {
					return nil, fmt.Errorf("RESP3 protocol violation: unexpected streamed string")
				}
				if len(line) < 4 {
					if eof {
						break
					}
					return nil, fmt.Errorf("RESP3 protocol violation: malformed streamed string chunk header")
				}
				size, errAtoi := parseSize(line[1 : len(line)-2])
				if errAtoi != nil {
					if eof {
						break
					}
					return nil, errAtoi
				}
				if size > 0 {
					// Expect one raw data line
					stack = append(stack, 1)
					bytesToRead = size
					raw = true
				} else {
					// Final chunk (;0\r\n), pop streamed string state
					stack = stack[:len(stack)-1]
				}

			case '*', '~', '%', '|', '>':
				// Collection types: Array, Set, Map, Attribute, Push
				if inStreamedString {
					return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
				}

				if len(line) >= 2 && line[0] != '>' && line[1] == '?' { // Streamed collection start (e.g., *?\r\n)
					if line[0] == '|' {
						stack = append(stack, 1) // Attributes prefix: one more message after stream
					}
					stack = append(stack, -1)
				} else { // Defined size collection (e.g., *3\r\n)
					if len(line) < 4 {
						if eof {
							break
						}
						return nil, fmt.Errorf("RESP3 protocol violation: malformed collection header")
					}
					size, errAtoi := parseSize(line[1 : len(line)-2])
					if errAtoi != nil {
						if eof {
							break
						}
						return nil, errAtoi
					}

					count := size
					// Hashes (%) and attributes (|) are pairs of keys+values
					if line[0] == '%' || line[0] == '|' {
						count *= 2
					}
					// Attributes (|) are prefixes; we must read one more complete message after the pairs
					if line[0] == '|' {
						count++
					}

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
				// Pop streamed collection state
				stack = stack[:len(stack)-1]

			default:
				// Inline commands or unknown protocol markers
				if !allowInline {
					return nil, fmt.Errorf("RESP3 protocol violation: unsupported item type \"%s\"", string(line[0]))
				}
			}
		}

		// Finished levels (remaining count == 0) are popped automatically.
		// This handles the end of fixed-size arrays/maps and the completion of raw bulk lines.
		for len(stack) > 0 && stack[len(stack)-1] == 0 {
			stack = stack[:len(stack)-1]
		}

		// Inline commands are only allowed as the very first line of a message
		allowInline = false
	}

	var err error = nil
	if eof {
		err = io.EOF
	}

	return r.buffer[r.messageStart:r.readPosition], err
}

// readFromSource fetches more data from the underlying reader into the internal buffer.
// It automatically grows or moves the buffer to ensure there is enough space for new data.
func (r *RedisProtocolReader) readFromSource() (int, error) {
	// Check if we need more space in the buffer.
	if cap(r.buffer)-len(r.buffer) < r.minimumReadSize {
		// If we are at the very start of a message, we must grow the buffer.
		if r.messageStart == 0 {
			r.buffer = slices.Grow(r.buffer, r.initialBufferSize)
		} else {
			// If we have already parsed some data, shift the unparsed portion to the beginning
			// of a new buffer to reuse space.
			newBuffer := make([]byte, len(r.buffer)-r.messageStart, max(len(r.buffer)-r.messageStart, r.initialBufferSize))
			copy(newBuffer, r.buffer[r.messageStart:])
			r.buffer = newBuffer
			// Adjust offsets accordingly.
			r.readPosition -= r.messageStart
			r.lineStart -= r.messageStart
			r.messageStart = 0
		}
	}

	// Read as much data as possible into the remaining capacity.
	start := len(r.buffer)
	n, err := r.reader.Read(r.buffer[start:cap(r.buffer)])

	// Update the slice length to include the new bytes.
	r.buffer = r.buffer[:start+n]

	return n, err
}

// readLine reads a single line (terminated by CRLF) from the buffer.
// For bulk payloads, it ensures that at least 'minimumBytes' are skipped before looking for the CRLF.
func (r *RedisProtocolReader) readLine(minimumBytes int) ([]byte, error) {
	r.lineStart = r.readPosition

	for ; r.readPosition <= len(r.buffer); r.readPosition++ {
		// If we've exhausted the buffer, fetch more data.
		if r.readPosition == len(r.buffer) {
			n, err := r.readFromSource()
			if n == 0 {
				// We reached EOF without finding a CRLF.
				return r.buffer[r.lineStart:], err
			} else if err != nil && err != io.EOF {
				return nil, err
			}
		}

		// Look for the \r\n terminator.
		// For bulk strings, we must have read at least the expected number of bytes (minimumBytes)
		// before we consider a \r\n as the end of the payload.
		if r.readPosition > 0 && r.buffer[r.readPosition] == '\n' && r.buffer[r.readPosition-1] == '\r' && r.readPosition-r.lineStart > minimumBytes {
			r.readPosition++
			return r.buffer[r.lineStart:r.readPosition], nil
		}
	}

	return nil, nil
}
