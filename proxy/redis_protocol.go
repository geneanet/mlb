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
	reader              io.Reader
	buffer              []byte
	read_position       int
	line_start          int
	message_start       int
	initial_buffer_size int
	minimum_read_size   int
}

func NewRedisProtocolReader(reader io.Reader, initial_buffer_size int) RedisProtocolReader {
	return RedisProtocolReader{
		reader:              reader,
		initial_buffer_size: initial_buffer_size,
		minimum_read_size:   64,
	}
}

func (r *RedisProtocolReader) ReadMessage(allow_inline bool) ([]byte, error) {
	lines_to_read := 1
	bytes_to_read := 0

	r.message_start = r.read_position
	eof := false
	raw := false
	streaming := 0
	str_streaming := false

	for lines_to_read > 0 && !eof {
		// Read a new line of data
		line, err := r.readLine(bytes_to_read)
		lines_to_read--
		bytes_to_read = 0
		if len(line) == 0 || err != nil && err != io.EOF {
			return nil, err
		} else if err == io.EOF {
			eof = true
		}

		// Line type
		switch line[0] {
		case '+', '-', ':', '_', ',', '#', '(':
			if str_streaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

		case '$', '!', '=':
			if str_streaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

			if line[0] == '$' && line[1] == '?' { // Streamed string
				str_streaming = true
				lines_to_read++
			} else { // Defined size
				size, errAtoi := strconv.Atoi(string(line[1 : len(line)-2]))
				if errAtoi != nil {
					return nil, errAtoi
				}

				if size >= 0 {
					lines_to_read++
					bytes_to_read = size
					raw = true
				}
			}

		case ';':
			if str_streaming {
				size, errAtoi := strconv.Atoi(string(line[1 : len(line)-2]))
				if errAtoi != nil {
					return nil, errAtoi
				}
				if size > 0 {
					lines_to_read += 2
					bytes_to_read = size
					raw = true
				} else {
					str_streaming = false
				}

			} else {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected streamed string")
			}

		case '*', '~', '%', '|', '>':
			if str_streaming {
				return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" during streamed string", string(line[0]))
			}

			if line[0] != '>' && line[1] == '?' { // Streamed
				streaming++
				lines_to_read++
			} else { // Defined size
				size, errAtoi := strconv.Atoi(string(line[1 : len(line)-2]))
				if errAtoi != nil {
					return nil, errAtoi
				}

				// Hashes and attibutes have keys+values
				if line[0] == '%' || line[0] == '|' {
					size *= 2
				}

				lines_to_read += size
			}

		case '.':
			if str_streaming {
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
			} else if !allow_inline {
				// Protocol error
				return nil, fmt.Errorf("RESP3 protocol violation: unsupported item type \"%s\"", string(line[0]))
			}

		}

		if streaming > 0 && lines_to_read == streaming-1 {
			lines_to_read++
		}

		// Inline commands only allowed if first line
		allow_inline = false
	}

	return r.buffer[r.message_start:r.read_position], nil
}

func (r *RedisProtocolReader) readFromSource() (int, error) {
	// If there is less than r.minimum_read_size room in the buffer
	if cap(r.buffer)-len(r.buffer) < r.minimum_read_size {
		// If the buffer contains only the message we are currently parsing, grow it.
		// Otherwise create a new buffer and copy the start of the message.
		if r.message_start == 0 {
			r.buffer = slices.Grow(r.buffer, r.initial_buffer_size)
		} else {
			new_buffer := make([]byte, len(r.buffer)-r.message_start, r.initial_buffer_size)
			copy(new_buffer, r.buffer[r.message_start:])
			r.buffer = new_buffer
			r.read_position -= r.message_start
			r.line_start -= r.message_start
			r.message_start = 0
		}
	}

	// Read data
	start := len(r.buffer)
	n, err := r.reader.Read(r.buffer[start:cap(r.buffer)])

	// Shrink the slice
	r.buffer = r.buffer[:start+n]

	return n, err
}

func (r *RedisProtocolReader) readLine(minimum_bytes int) ([]byte, error) {
	r.line_start = r.read_position

	for ; r.read_position <= len(r.buffer); r.read_position++ {
		// We have reached the end of the buffer without finding the end of the line
		if r.read_position == len(r.buffer) {
			// Fetch some more data
			n, err := r.readFromSource()
			if n == 0 {
				return r.buffer[r.line_start:], err
			} else if err != nil && err != io.EOF {
				return nil, err
			}
		}

		// \r\n found
		if r.read_position > 0 && r.buffer[r.read_position] == '\n' && r.buffer[r.read_position-1] == '\r' && r.read_position-r.line_start > minimum_bytes {
			r.read_position++
			return r.buffer[r.line_start:r.read_position], nil
		}
	}

	// Should never be reached
	return nil, nil
}
