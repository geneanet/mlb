package memcache

import (
	"bufio"
	"bytes"
	"io"
	"sync"
)

var protocolReaderPool sync.Pool
var protocolWriterPool sync.Pool

// MemcacheProtocolWriter provides a high-level writer for Memcache protocol.
type MemcacheProtocolWriter struct {
	*bufio.Writer
}

// NewMemcacheProtocolWriter creates a new writer, potentially reusing one from the pool.
func NewMemcacheProtocolWriter(writer io.Writer, bufferSize int) *MemcacheProtocolWriter {
	if v := protocolWriterPool.Get(); v != nil {
		pw := v.(*MemcacheProtocolWriter)
		pw.Reset(writer)
		return pw
	}
	return &MemcacheProtocolWriter{
		Writer: bufio.NewWriterSize(writer, bufferSize),
	}
}

// Release returns the internal bufio.Writer to the pool.
func (pw *MemcacheProtocolWriter) Release() {
	pw.Reset(nil)
	protocolWriterPool.Put(pw)
}

// MemcacheProtocolReader handles reading from a memcache connection with minimal allocations.
// It uses an internal buffer to accumulate data and provides methods for reading
// lines and fixed-size payloads efficiently.
// ponytail: reusing the whole reader struct and its internal buffer via sync.Pool.
type MemcacheProtocolReader struct {
	br     *bufio.Reader
	buffer []byte
}

// NewMemcacheProtocolReader acquires a MemcacheProtocolReader from the pool or creates a new one.
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

// Release returns the reader to the pool.
func (r *MemcacheProtocolReader) Release() {
	if r.br != nil {
		r.br.Reset(nil)
	}
	// ponytail: if the buffer grew too large, drop it to avoid leaking memory in the pool
	if cap(r.buffer) > 64*1024 {
		r.buffer = nil
	}
	protocolReaderPool.Put(r)
}

// ReadLine reads a line terminated by LF (and optionally CR) and returns it.
// The returned slice is valid until the next read call on the same reader.
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

// ReadFull reads exactly n bytes into the internal buffer and returns a slice.
// It handles buffer growth if necessary.
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

var responseChanPool = sync.Pool{
	New: func() any {
		return make(chan MemcacheResponse, 1)
	},
}

// getResponseChan acquires a response channel from the pool and ensures it's empty.
func getResponseChan() chan MemcacheResponse {
	v := responseChanPool.Get()

	ch := v.(chan MemcacheResponse)
	// ponytail: drain the channel just in case a response arrived after the previous query was abandoned.
	select {
	case <-ch:
	default:
	}
	return ch
}

// putResponseChan returns a response channel to the pool.
func putResponseChan(ch chan MemcacheResponse) {
	responseChanPool.Put(ch)
}

var bufferPool = sync.Pool{
	New: func() any {
		return bytes.NewBuffer(make([]byte, 0, 4096))
	},
}

// ReleaseBuffer returns a buffer to the pool.
func ReleaseBuffer(b *bytes.Buffer) {
	// Do not put the buffer back in the pool if too small or too big
	if b == nil || b.Cap() < 4096 || b.Cap() > 64*1024 {
		return
	}
	b.Reset()
	bufferPool.Put(b)
}

// getFields splits a line into fields by spaces, reusing a slice from a pool.
// It mimics bytes.Fields but with zero allocations if the pooled slice is large enough.
func (p *MemcacheProxy) getFields(line []byte) *[][]byte {
	dst := p.fieldsPool.Get().(*[][]byte)
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

func (p *MemcacheProxy) releaseFields(f *[][]byte) {
	// ponytail: if the slice grew too large, drop it to avoid leaking memory in the pool
	if cap(*f) > 1024 {
		return
	}
	p.fieldsPool.Put(f)
}
