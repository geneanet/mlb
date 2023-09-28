package proxy

import (
	"bytes"
	"fmt"
	"mlb/misc"
	"strconv"
	"strings"
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
	// Error handler
	defer func() {
		if r := recover(); r != nil {
			e = misc.EnsureError(r)
		}
	}()

	q.response_chan <- RedisReponse{
		query: q,
		item:  nil,
	}

	return nil
}

func (q RedisQuery) IsRestricted() (bool, string) {
	command, err := q.GetCommand()

	if err != nil {
		return false, ""
	}

	restrictedCommands := [...]string{
		"watch", "unwatch", "multi", "exec", "discard", // MULTI
		"brpoplpush", "blpop", "brpop", "bzpopmin", "bzpopmax", "xread", "xreadgroup", "wait", "waitaof", // BLOCKING
		"subscribe", "unsubscribe", "psubscribe", "punsubscribe", "ssubscribe", "sunsubscribe", "publish", "spublish", "pubsub", // PUBSUB
	}

	for _, restrictedCommand := range restrictedCommands {
		if len(command) == len(restrictedCommand) && strings.EqualFold(command, restrictedCommand) {
			return false, command
		}
	}

	return true, command
}

func (q RedisQuery) GetCommand() (string, error) {
	i := bytes.IndexByte(q.item, '$')
	if i == -1 {
		return "", fmt.Errorf("bulk string start not found")
	}

	j := bytes.IndexByte(q.item[i:], '\r')
	if j == -1 {
		return "", fmt.Errorf("bulk string end not found")
	}

	size, err := strconv.Atoi(string(q.item[i+1 : i+j]))
	if err != nil {
		return "", fmt.Errorf("unable to parse bulk string size: %v", err)
	}

	return string(q.item[i+j+2 : i+j+2+size]), nil
}

//---------------
// Redis Response
//---------------

type RedisReponse struct {
	query RedisQuery
	item  []byte
}
