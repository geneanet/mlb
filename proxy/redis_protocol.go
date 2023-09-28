package proxy

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
)

//-----------------
// Protocol parsing
//-----------------

// TODO: improve perfs by using buffer pools and avoiding copies

func redisReadFullLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadBytes('\n')
	if err != nil || (len(line) >= 2 && line[len(line)-2] == '\r') {
		return line, err
	}

	fullLine := bytes.NewBuffer(line)
	for {
		line, err := reader.ReadBytes('\n')
		fullLine.Write(line)
		if err != nil || (fullLine.Len() >= 2 && fullLine.Bytes()[fullLine.Len()-2] == '\r') {
			return fullLine.Bytes(), err
		}
	}
}

func redisReadItem(reader *bufio.Reader, allow_inline bool) ([]byte, error) {
	data, err := redisReadFullLine(reader)

	if err != nil || len(data) == 0 {
		return nil, err
	}

	switch data[0] {
	case '+', '-', ':', '_', ',', '#', '(', '.':
		return data, nil

	case '$', '!', '=', ';':
		if data[0] == '$' && data[1] == '?' { // Streamed string
			fullData := bytes.NewBuffer(data)
			for {
				item, err := redisReadItem(reader, false)
				if err != nil {
					return nil, err
				}
				if item[0] != ';' {
					return nil, fmt.Errorf("RESP3 protocol violation: unexpected item type \"%s\" while reading streamed string", string(item[0]))
				}

				fullData.Write(item)

				// Last item should be an zero-size part
				if item[1] == '0' && item[2] == '\r' && item[3] == '\n' {
					break
				}
			}
			return fullData.Bytes(), nil
		} else { // Defined size
			size, errAtoi := strconv.Atoi(string(data[1 : len(data)-2]))
			if errAtoi != nil {
				return nil, errAtoi
			}
			if size == -1 {
				return data, nil
			}

			buffer := make([]byte, size+2)
			_, err := io.ReadFull(reader, buffer)
			if err != nil {
				return nil, err
			}
			if buffer[len(buffer)-2] != '\r' || buffer[len(buffer)-1] != '\n' {
				return nil, fmt.Errorf("RESP3 protocol violation: bulk item not terminated by \\r\\n")
			}

			fullData := bytes.NewBuffer(data)
			fullData.Write(buffer)

			return fullData.Bytes(), nil
		}

	case '*', '~', '%', '|', '>':
		if data[0] != '>' && data[1] == '?' { // Streamed
			fullData := bytes.NewBuffer(data)
			for {
				item, err := redisReadItem(reader, false)
				if err != nil {
					return nil, err
				}

				fullData.Write(item)

				// Last item is ".\r\n"
				if item[0] == '.' && item[1] == '\r' && item[2] == '\n' {
					break
				}
			}
			return fullData.Bytes(), nil
		} else { // Defined size

			size, errAtoi := strconv.Atoi(string(data[1 : len(data)-2]))
			if errAtoi != nil {
				return nil, errAtoi
			}

			// Hashes and attibutes have keys+values
			if data[0] == '%' || data[0] == '|' {
				size *= 2
			}

			fullData := bytes.NewBuffer(data)
			for i := 0; i < size; i++ {
				item, err := redisReadItem(reader, false)
				if err != nil {
					return nil, err
				}
				fullData.Write(item)
			}
			return fullData.Bytes(), nil
		}
	default:
		if allow_inline {
			// Convert inline command to RESP
			converted := bytes.Buffer{}
			items := bytes.Split(data[0:len(data)-2], []byte(" "))
			converted.WriteString(fmt.Sprintf("*%d\r\n", len(items)))
			for i := range items {
				converted.WriteString(fmt.Sprintf("$%d\r\n", len(items[i])))
				converted.Write(items[i])
				converted.WriteString("\r\n")
			}
			return converted.Bytes(), nil
		} else {
			// Protocol error
			return nil, fmt.Errorf("RESP3 protocol violation: unsupported item type \"%s\"", string(data[0]))
		}
	}
}
