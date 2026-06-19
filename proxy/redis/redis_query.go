package redis

import (
	"bytes"
	"fmt"
	"sync/atomic"
)

//------------
// Redis Query
//------------

var RedisQueryCounter atomic.Uint64

var restrictedCommandsMap = map[string]struct{}{
	"watch": {}, "unwatch": {}, "multi": {}, "exec": {}, "discard": {}, // MULTI
	"brpoplpush": {}, "blpop": {}, "brpop": {}, "bzpopmin": {}, "bzpopmax": {}, "xread": {}, "xreadgroup": {}, "wait": {}, "waitaof": {}, // BLOCKING
	"subscribe": {}, "unsubscribe": {}, "psubscribe": {}, "punsubscribe": {}, "ssubscribe": {}, "sunsubscribe": {}, "publish": {}, "spublish": {}, "pubsub": {}, // PUBSUB
	"monitor": {}, // MISC
}

// RedisQuery represents a single Redis command query.
type RedisQuery struct {
	id               uint64
	item             []byte
	responseChan     chan RedisReponse
	responseChanStop chan struct{}
}

// NewRedisQuery creates a new RedisQuery with a unique ID.
func NewRedisQuery(item []byte, responseChan chan RedisReponse, responseChanStop chan struct{}) RedisQuery {
	return RedisQuery{
		id:               RedisQueryCounter.Add(1),
		item:             item,
		responseChan:     responseChan,
		responseChanStop: responseChanStop,
	}
}

// Reply sends the backend response back to the client.
func (q RedisQuery) Reply(item []byte) (e error) {
	select {
	case q.responseChan <- RedisReponse{
		query: q,
		item:  item,
	}:
		return nil
	case <-q.responseChanStop:
		return fmt.Errorf("response channel is closed")
	}
}

// Abort sends a nil response to the client, effectively aborting the query.
func (q RedisQuery) Abort() (e error) {
	return q.Reply(nil)
}

// IsRestricted checks if the command is in the restricted commands list.
// Restricted commands are those that would break the proxy logic (e.g. blocking or pubsub).
func (q RedisQuery) IsRestricted() bool {
	command, err := q.GetCommand()

	// Invalid commands are denied
	if err != nil {
		return true
	}

	// If the command is longer than 32 bytes, it's definitely not in our restricted list
	if len(command) > 32 {
		return false
	}

	// Efficiently convert to lowercase into a stack buffer
	var buf [32]byte
	for i, b := range command {
		if b >= 'A' && b <= 'Z' {
			buf[i] = b + 32
		} else {
			buf[i] = b
		}
	}

	_, found := restrictedCommandsMap[string(buf[:len(command)])]
	return found
}

// GetCommand extracts the command name from the raw query bytes.
func (q RedisQuery) GetCommand() ([]byte, error) {
	if len(q.item) >= 3 { // Minimum 1 character + \r\n
		if q.item[0] == '*' { // Array
			i := bytes.IndexByte(q.item, '$')
			if i == -1 {
				return []byte{}, fmt.Errorf("bulk string start not found")
			}

			j := bytes.IndexByte(q.item[i:], '\r')
			if j == -1 {
				return []byte{}, fmt.Errorf("bulk string end not found")
			}

			size, err := parseSize(q.item[i+1 : i+j])
			if err != nil {
				return []byte{}, fmt.Errorf("unable to parse bulk string size: %v", err)
			}

			return q.item[i+j+2 : i+j+2+size], nil

		} else { // Inline query
			space := bytes.IndexByte(q.item, ' ')
			if space == -1 {
				return q.item[:len(q.item)-2], nil
			} else {
				return q.item[:space], nil
			}
		}
	} else {
		return []byte{}, fmt.Errorf("invalid command")
	}
}

//---------------
// Redis Response
//---------------

// RedisReponse represents a response from a Redis backend for a specific query.
type RedisReponse struct {
	query RedisQuery
	item  []byte
}
