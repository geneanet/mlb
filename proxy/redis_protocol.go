package proxy

import (
	"fmt"
	"io"
	"slices"
	"strconv"
)

//-----------------
// Protocol parsing
//-----------------

// TODO: improve perfs by using buffer pools

type RedisProtocolReader struct {
	reader            io.Reader
	buffer            []byte
	readPosition      int
	lineStart         int
	messageStart      int
	initialBufferSize int
	minimumReadSize   int
}

func NewRedisProtocolReader(reader io.Reader, initialBufferSize int) RedisProtocolReader {
	return RedisProtocolReader{
		reader:            reader,
		initialBufferSize: initialBufferSize,
		minimumReadSize:   64,
	}
}

func (r *RedisProtocolReader) ReadMessage(allowInline bool) ([]byte, error) {
	linesToRead := 1
	bytesToRead := 0

	r.messageStart = r.readPosition
	eof := false
	raw := false
	streaming := 0
	strStreaming := false

	for linesToRead > 0 && !eof {
		// Read a new line of data
		line, err := r.readLine(bytesToRead)
		linesToRead--
		bytesToRead = 0
		if len(line) == 0 || err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			eof = true
		}

		// Line type
		switch line[0] {
		case '+', '-', ':', '_', ',', '#', '(':
			if strStreaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

		case '$', '!', '=':
			if strStreaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

			if line[0] == '$' && line[1] == '?' { // Streamed string
				strStreaming = true
				linesToRead++
			} else { // Defined size
				size, errAtoi := strconv.Atoi(string(line[1 : len(line)-2]))
				if errAtoi != nil {
					return nil, errAtoi
				}

				if size >= 0 {
					linesToRead++
					bytesToRead = size
					raw = true
				}
			}

		case ';':
			if strStreaming {
				size, errAtoi := strconv.Atoi(string(line[1 : len(line)-2]))
				if errAtoi != nil {
					return nil, errAtoi
				}
				if size > 0 {
					linesToRead += 2
					bytesToRead = size
					raw = true
				} else {
					strStreaming = false
				}

			} else {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected streamed string")
			}

		case '*', '~', '%', '|', '>':
			if strStreaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

			if line[0] != '>' && line[1] == '?' { // Streamed
				streaming++
				linesToRead++
			} else { // Defined size
				size, errAtoi := strconv.Atoi(string(line[1 : len(line)-2]))
				if errAtoi != nil {
					return nil, errAtoi
				}

				// Hashes and attibutes have keys+values
				if line[0] == '%' || line[0] == '|' {
					size *= 2
				}

				linesToRead += size
			}

		case '.':
			if strStreaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}
			if streaming == 0 {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item . while not streaming")
			}

			// End of streaming
			if line[1] == '\r' && line[2] == '\n' {
				streaming--
			}

		default:
			if raw {
				// Raw mode only valid for one line
				raw = false
			} else if !allowInline {
				// Protocol error
				return nil, fmt.Errorf("RESP3 protocol violation: unsupported item type \"%s\"", string(line[0]))
			}

		}

		if streaming > 0 && linesToRead == streaming-1 {
			linesToRead++
		}

		// Inline commands only allowed if first line
		allowInline = false
	}

	return r.buffer[r.messageStart:r.readPosition], nil
}

func (r *RedisProtocolReader) readFromSource() (int, error) {
	// If there is less than r.minimumReadSize room in the buffer
	if cap(r.buffer)-len(r.buffer) < r.minimumReadSize {
		// If the buffer contains only the message we are currently parsing, grow it.
		// Otherwise create a new buffer and copy the start of the message.
		if r.messageStart == 0 {
			r.buffer = slices.Grow(r.buffer, r.initialBufferSize)
		} else {
			newBuffer := make([]byte, len(r.buffer)-r.messageStart, r.initialBufferSize)
			copy(newBuffer, r.buffer[r.messageStart:])
			r.buffer = newBuffer
			r.readPosition -= r.messageStart
			r.lineStart -= r.messageStart
			r.messageStart = 0
		}
	}

	// Read data
	start := len(r.buffer)
	n, err := r.reader.Read(r.buffer[start:cap(r.buffer)])

	// Shrink the slice
	r.buffer = r.buffer[:start+n]

	return n, err
}

func (r *RedisProtocolReader) readLine(minimumBytes int) ([]byte, error) {
	r.lineStart = r.readPosition

	for ; r.readPosition <= len(r.buffer); r.readPosition++ {
		// We have reached the end of the buffer without finding the end of the line
		if r.readPosition == len(r.buffer) {
			// Fetch some more data
			n, err := r.readFromSource()
			if n == 0 {
				return r.buffer[r.lineStart:], err
			} else if err != nil && err != io.EOF {
				return nil, err
			}
		}

		// \r\n found
		if r.readPosition > 0 && r.buffer[r.readPosition] == '\n' && r.buffer[r.readPosition-1] == '\r' && r.readPosition-r.lineStart > minimumBytes {
			r.readPosition++
			return r.buffer[r.lineStart:r.readPosition], nil
		}
	}

	// Should never be reached
	return nil, nil
}
