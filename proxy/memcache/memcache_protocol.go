package memcache

import (
	"bufio"
	"bytes"
	"io"
	"sync"
)

var protocolReaderPool sync.Pool

// MemcacheProtocolReader handles reading from a memcache connection with minimal allocations.
// ponytail: reusing the whole reader struct and its internal buffer.
type MemcacheProtocolReader struct {
	br     *bufio.Reader
	buffer []byte
}

func NewMemcacheProtocolReader(r io.Reader, bufferSize int) *MemcacheProtocolReader {
	var pr *MemcacheProtocolReader
	if v := protocolReaderPool.Get(); v != nil {
		pr = v.(*MemcacheProtocolReader)
		pr.br.Reset(r)
	} else {
		pr = &MemcacheProtocolReader{
			br:     bufio.NewReaderSize(r, bufferSize),
			buffer: make([]byte, 0, bufferSize),
		}
	}
	pr.buffer = pr.buffer[:0]
	return pr
}

func (r *MemcacheProtocolReader) Release() {
	if r.br != nil {
		r.br.Reset(nil)
	}
	protocolReaderPool.Put(r)
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
	// ponytail: grow buffer without allocations using cap
	if cap(r.buffer)-start < n {
		newCap := max(cap(r.buffer)*2, start+n)
		newBuf := make([]byte, start, newCap)
		copy(newBuf, r.buffer)
		r.buffer = newBuf
	}
	r.buffer = r.buffer[:start+n]
	_, err := io.ReadFull(r.br, r.buffer[start:])
	return r.buffer[start:], err
}

var bufferPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

var fieldsPool = sync.Pool{
	New: func() any {
		f := make([][]byte, 0, 16)
		return &f
	},
}

// getFields splits a line into fields by spaces, reusing a slice from a pool.
// It mimics bytes.Fields but with zero allocations if the pooled slice is large enough.
func getFields(line []byte) *[][]byte {
	dst := fieldsPool.Get().(*[][]byte)
	*dst = (*dst)[:0]
	start := 0
	for i, b := range line {
		if b == ' ' || b == '\r' || b == '\n' {
			if i > start {
				*dst = append(*dst, line[start:i])
			}
			start = i + 1
		}
	}
	if start < len(line) {
		*dst = append(*dst, line[start:])
	}
	return dst
}

func releaseFields(f *[][]byte) {
	fieldsPool.Put(f)
}
