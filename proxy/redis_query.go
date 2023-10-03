package proxy

import (
	"bytes"
	"fmt"
	"strconv"
	"sync/atomic"
)

//------------
// Redis Query
//------------

var RedisQueryCounter atomic.Uint64

type RedisQuery struct {
	id                 uint64
	item               []byte
	response_chan      chan RedisReponse
	response_chan_stop chan struct{}
}

func NewRedisQuery(item []byte, response_chan chan RedisReponse, response_chan_stop chan struct{}) RedisQuery {
	return RedisQuery{
		id:                 RedisQueryCounter.Add(1),
		item:               item,
		response_chan:      response_chan,
		response_chan_stop: response_chan_stop,
	}
}

func (q RedisQuery) Reply(item []byte) (e error) {
	select {
	case q.response_chan <- RedisReponse{
		query: q,
		item:  item,
	}:
		return nil
	case <-q.response_chan_stop:
		return fmt.Errorf("response channel is closed")
	}
}

func (q RedisQuery) Abort() (e error) {
	return q.Reply(nil)
}

func (q RedisQuery) IsAllowed() bool {
	command, err := q.GetCommand()

	if err != nil {
		return false
	}

	restrictedCommands := [...][]byte{
		[]byte("watch"), []byte("unwatch"), []byte("multi"), []byte("exec"), []byte("discard"), // MULTI
		[]byte("brpoplpush"), []byte("blpop"), []byte("brpop"), []byte("bzpopmin"), []byte("bzpopmax"), []byte("xread"), []byte("xreadgroup"), []byte("wait"), []byte("waitaof"), // BLOCKING
		[]byte("subscribe"), []byte("unsubscribe"), []byte("psubscribe"), []byte("punsubscribe"), []byte("ssubscribe"), []byte("sunsubscribe"), []byte("publish"), []byte("spublish"), []byte("pubsub"), // PUBSUB
		[]byte("monitor"), // MISC
	}

	for _, restrictedCommand := range restrictedCommands {
		if len(command) == len(restrictedCommand) && bytes.EqualFold(command, restrictedCommand) {
			return false
		}
	}

	return true
}

func (q RedisQuery) GetCommand() ([]byte, error) {
	if len(q.item) >= 3 { // Minimum 1 character + \r\n
		if q.item[0] == '@' { // Array
			i := bytes.IndexByte(q.item, '$')
			if i == -1 {
				return []byte{}, fmt.Errorf("bulk string start not found")
			}

			j := bytes.IndexByte(q.item[i:], '\r')
			if j == -1 {
				return []byte{}, fmt.Errorf("bulk string end not found")
			}

			size, err := strconv.Atoi(string(q.item[i+1 : i+j]))
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

type RedisReponse struct {
	query RedisQuery
	item  []byte
}
