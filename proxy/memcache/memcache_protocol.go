package memcache

import (
	"bufio"
	"bytes"
	"io"
	"sync"
)

var readerPool sync.Pool

// MemcacheProtocolReader handles reading from a memcache connection with minimal allocations.
// ponytail: reusing bufio.Reader and an accumulation buffer.
type MemcacheProtocolReader struct {
	br     *bufio.Reader
	buffer []byte
}

func NewMemcacheProtocolReader(r io.Reader, bufferSize int) *MemcacheProtocolReader {
	var br *bufio.Reader
	if v := readerPool.Get(); v != nil {
		br = v.(*bufio.Reader)
		br.Reset(r)
	} else {
		br = bufio.NewReaderSize(r, bufferSize)
	}
	return &MemcacheProtocolReader{
		br:     br,
		buffer: make([]byte, 0, bufferSize),
	}
}

func (r *MemcacheProtocolReader) Release() {
	if r.br != nil {
		r.br.Reset(nil)
		readerPool.Put(r.br)
		r.br = nil
	}
}

// ReadLine reads a CRLF terminated line and returns it.
// The returned slice is valid until the next read call.
func (r *MemcacheProtocolReader) ReadLine() ([]byte, error) {
	r.buffer = r.buffer[:0]
	for {
		line, err := r.br.ReadSlice('\n')
		r.buffer = append(r.buffer, line...)
		if err != bufio.ErrBufferFull {
			return r.buffer, err
		}
	}
}

// ReadFull reads exactly n bytes into the internal buffer.
func (r *MemcacheProtocolReader) ReadFull(n int) ([]byte, error) {
	start := len(r.buffer)
	r.buffer = append(r.buffer, make([]byte, n)...)
	_, err := io.ReadFull(r.br, r.buffer[start:])
	return r.buffer[start:], err
}

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}
