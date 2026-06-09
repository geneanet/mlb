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
	linesToRead := 1 // Number of logical lines/items remaining to complete the current message
	bytesToRead := 0 // Number of raw bytes to read if the next line is a fixed-size bulk payload

	r.messageStart = r.readPosition
	eof := false
	raw := false                         // True if we are expecting a raw data line (e.g., after $5\r\n)
	streaming := 0                       // Depth of nested streamed collections (e.g., *?\r\n)
	streamingAttributes := 0             // Number of active streamed attributes requiring an extra following message
	streamingStack := make([]bool, 0, 8) // Stack to track if a streamed collection is an attribute
	strStreaming := false                // True if we are currently reading a streamed bulk string ($?\r\n)

	// Continue reading as long as we have pending items or active streamed collections
	for (linesToRead > 0 || streaming > 0) && !eof {
		// Read a new line of data from the source/buffer
		line, err := r.readLine(bytesToRead)
		linesToRead--
		bytesToRead = 0
		if len(line) == 0 || err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			eof = true
		}

		// Parse the line type based on the first character (RESP marker)
		switch line[0] {
		case '+', '-', ':', '_', ',', '#', '(':
			// Simple types: String, Error, Integer, Null, Double, BigNumber, Boolean, Verbatim (short)
			if strStreaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

		case '$', '!', '=':
			// Fixed-size types: Bulk String, Bulk Error, Verbatim String
			if strStreaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

			if line[0] == '$' && line[1] == '?' { // Streamed string start ($?\r\n)
				strStreaming = true
				linesToRead++
			} else { // Defined size (e.g., $12\r\n)
				size, errAtoi := parseSize(line[1 : len(line)-2])
				if errAtoi != nil {
					return nil, errAtoi
				}

				if size >= 0 {
					// We expect one more "line" which is the raw data of 'size' bytes
					linesToRead++
					bytesToRead = size
					raw = true
				}
			}

		case ';':
			// Streamed string chunk (;size\r\ndata\r\n)
			if strStreaming {
				size, errAtoi := parseSize(line[1 : len(line)-2])
				if errAtoi != nil {
					return nil, errAtoi
				}
				if size > 0 {
					// Expect the raw data line and then potentially more chunks
					linesToRead += 2
					bytesToRead = size
					raw = true
				} else {
					// Final chunk (;0\r\n)
					strStreaming = false
				}

			} else {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected streamed string")
			}

		case '*', '~', '%', '|', '>':
			// Collection types: Array, Set, Map, Attribute, Push
			if strStreaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

			if line[0] != '>' && line[1] == '?' { // Streamed collection start (e.g., *?\r\n)
				streaming++
				isAttr := line[0] == '|'
				streamingStack = append(streamingStack, isAttr)
				if isAttr {
					streamingAttributes++
				}
			} else { // Defined size collection (e.g., *3\r\n)
				size, errAtoi := parseSize(line[1 : len(line)-2])
				if errAtoi != nil {
					return nil, errAtoi
				}

				// Hashes (%) and attributes (|) are pairs of keys+values
				if line[0] == '%' || line[0] == '|' {
					size *= 2
				}

				linesToRead += size
				// Attributes (|) are prefixes; we must read one more complete message after the pairs
				if line[0] == '|' {
					linesToRead++
				}
			}

		case '.':
			// Streamed collection terminator (.\r\n)
			if strStreaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \".\" during streamed string")
			}
			if streaming == 0 {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item . while not streaming")
			}

			// Pop from the streaming stack
			if line[1] == '\r' && line[2] == '\n' {
				isAttr := streamingStack[len(streamingStack)-1]
				streamingStack = streamingStack[:len(streamingStack)-1]
				if isAttr {
					streamingAttributes--
				}
				streaming--
			}

		default:
			if raw {
				// We just read the raw data payload line for a bulk type
				raw = false
			} else if !allowInline {
				// Protocol error: unknown marker
				return nil, fmt.Errorf("RESP3 protocol violation: unsupported item type \"%s\"", string(line[0]))
			}

		}

		// Ensure that while we are in a streamed collection, we don't finish the message
		// prematurely if there are still open collection markers or pending attribute suffixes.
		if streaming > 0 && linesToRead < streaming+streamingAttributes {
			linesToRead = streaming + streamingAttributes
		}

		// Inline commands (no marker) are only allowed as the very first line of a message
		allowInline = false
	}

	return r.buffer[r.messageStart:r.readPosition], nil
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
