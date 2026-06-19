package memcache

import (
	"bytes"
	"io"
	"sync"
	"testing"
)

func TestMemcacheProtocolReader_ReadLine(t *testing.T) {
	data := "line1\r\nline2\nline3\r\n"
	r := NewMemcacheProtocolReader(bytes.NewReader([]byte(data)), 10)
	defer r.Release()

	l, err := r.ReadLine()
	if err != nil || string(l) != "line1\r\n" {
		t.Errorf("Expected line1\r\n, got %q (err: %v)", string(l), err)
	}

	l, err = r.ReadLine()
	if err != nil || string(l) != "line2\n" {
		t.Errorf("Expected line2\n, got %q (err: %v)", string(l), err)
	}

	l, err = r.ReadLine()
	if err != nil || string(l) != "line3\r\n" {
		t.Errorf("Expected line3\r\n, got %q (err: %v)", string(l), err)
	}

	_, err = r.ReadLine()
	if err != io.EOF {
		t.Errorf("Expected EOF, got %v", err)
	}
}

func TestMemcacheProtocolReader_ReadFull(t *testing.T) {
	data := "some data here"
	r := NewMemcacheProtocolReader(bytes.NewReader([]byte(data)), 5)
	defer r.Release()

	b, err := r.ReadFull(4)
	if err != nil || string(b) != "some" {
		t.Errorf("Expected 'some', got %q (err: %v)", string(b), err)
	}

	b, err = r.ReadFull(5)
	if err != nil || string(b) != " data" {
		t.Errorf("Expected ' data', got %q (err: %v)", string(b), err)
	}

	// Test growth
	b, err = r.ReadFull(5)
	if err != nil || string(b) != " here" {
		t.Errorf("Expected ' here', got %q (err: %v)", string(b), err)
	}
}

func TestGetFields(t *testing.T) {
	p := &MemcacheProxy{
		backendMinConnections: 1,
		backendMaxConnections: 1,
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
	}
	line := []byte("get key1 key2  key3\r\n")
	fieldsPtr := p.getFields(line)
	fields := *fieldsPtr
	defer p.releaseFields(fieldsPtr)

	expected := []string{"get", "key1", "key2", "key3"}
	if len(fields) != len(expected) {
		t.Fatalf("Expected %d fields, got %d", len(expected), len(fields))
	}

	for i, v := range expected {
		if string(fields[i]) != v {
			t.Errorf("Field %d: expected %q, got %q", i, v, string(fields[i]))
		}
	}
}

func TestReadMemcacheResponseFull(t *testing.T) {
	p := &MemcacheProxy{
		backendMinConnections: 1,
		backendMaxConnections: 1,
		fieldsPool: &sync.Pool{
			New: func() any {
				f := make([][]byte, 0, 16)
				return &f
			},
		},
	}
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"Simple", "STORED\r\n", "STORED\r\n"},
		{"Value", "VALUE k 0 2\r\nv1\r\nEND\r\n", "VALUE k 0 2\r\nv1\r\nEND\r\n"},
		{"ValueMulti", "VALUE k1 0 2\r\nv1\r\nVALUE k2 0 2\r\nv2\r\nEND\r\n", "VALUE k1 0 2\r\nv1\r\nVALUE k2 0 2\r\nv2\r\nEND\r\n"},
		{"Stats", "STAT items 1\r\nSTAT bytes 100\r\nEND\r\n", "STAT items 1\r\nSTAT bytes 100\r\nEND\r\n"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := NewMemcacheProtocolReader(bytes.NewReader([]byte(tt.input)), 1024)
			defer r.Release()
			buf := new(bytes.Buffer)
			err := p.readMemcacheResponseFull(r, buf)
			if err != nil {
				t.Fatal(err)
			}
			if buf.String() != tt.expected {
				t.Errorf("Expected %q, got %q", tt.expected, buf.String())
			}
		})
	}
}
