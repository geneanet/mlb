package main

import (
	"io"
	"net"
	"net/netip"
	"sync"

	"github.com/rs/zerolog/log"
)

type Proxy struct {
	address        string
	backend_tag    string
	backend_status string
	directory      *BackendDirectory
	running        bool
}

func newProxy(address string, backend_tag string, backend_status string, directory *BackendDirectory) *Proxy {
	return &Proxy{
		address:        address,
		backend_tag:    backend_tag,
		backend_status: backend_status,
		directory:      directory,
		running:        false,
	}
}

func (p *Proxy) start(wg *sync.WaitGroup) {
	if p.running {
		return
	}

	p.running = true
	wg.Add(1)

	listen, err := net.Listen("tcp", p.address)
	panicIfErr(err)

	log.Info().Str("address", p.address).Msg("Opening Frontend socket")

	go func() {
		defer listen.Close()
		defer wg.Done()
		defer func() { p.running = false }()

		for {
			conn, err := listen.Accept()
			panicIfErr(err)
			go p._handle_connection(conn)
		}
	}()
}

func (p *Proxy) _pipe(input net.Conn, output net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			log.Error().Str("input", input.RemoteAddr().String()).Str("output", output.RemoteAddr().String()).Err(r.(error)).Msg("Error while processing pipe")
		}
	}()

	buffer := make([]byte, 64*1024)

	for {
		nbytes, err := input.Read(buffer)
		if err == io.EOF {
			return
		}
		panicIfErr(err)
		_, err = output.Write(buffer[:nbytes])
		panicIfErr(err)
	}
}

func (p *Proxy) _handle_connection(conn_front net.Conn) {
	log.Debug().Str("address", conn_front.RemoteAddr().String()).Msg("Accepting Frontend connection")
	defer conn_front.Close()
	defer log.Debug().Str("address", conn_front.RemoteAddr().String()).Msg("Closing Frontend connection")

	err := conn_front.(*net.TCPConn).SetNoDelay(true)
	panicIfErr(err)

	// Error handler
	defer func() {
		if r := recover(); r != nil {
			log.Error().Str("peer", conn_front.RemoteAddr().String()).Err(r.(error)).Msg("Error while processing connection")
		}
	}()

	backend_address, err := p.directory.getBackend(p.backend_tag, p.backend_status)
	panicIfErr(err)

	backend_addrport, err := netip.ParseAddrPort(backend_address)
	panicIfErr(err)

	log.Debug().Str("address", backend_address).Msg("Opening Backend connection")
	conn_back, err := net.DialTCP("tcp", nil, net.TCPAddrFromAddrPort(backend_addrport))
	panicIfErr(err)
	defer conn_back.Close()
	defer log.Debug().Str("address", backend_address).Msg("Closing Backend connection")

	err = conn_back.SetNoDelay(true)
	panicIfErr(err)

	var wg sync.WaitGroup

	wg.Add(2)
	go p._pipe(conn_front, conn_back, &wg)
	go p._pipe(conn_back, conn_front, &wg)

	wg.Wait()
}
