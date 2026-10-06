package testutil

import (
	"net"
	"sync"
	"sync/atomic"
)

// Proxy is a TCP forwarder that can blackhole traffic: while blackholed,
// connections stay open but every byte in either direction is dropped, the
// way a network partition looks to both ends.
type Proxy struct {
	ln        net.Listener
	target    string
	blackhole atomic.Bool

	mu    sync.Mutex
	conns []net.Conn
}

// StartProxy listens on a free local port and forwards to target.
func StartProxy(target string) (*Proxy, error) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	p := &Proxy{ln: ln, target: target}
	go p.accept()
	return p, nil
}

// Addr is the host:port clients should dial.
func (p *Proxy) Addr() string {
	return p.ln.Addr().String()
}

// Blackhole starts (true) or stops (false) dropping traffic.
func (p *Proxy) Blackhole(on bool) {
	p.blackhole.Store(on)
}

// Close stops accepting and closes every proxied connection.
func (p *Proxy) Close() {
	_ = p.ln.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, c := range p.conns {
		_ = c.Close()
	}
}

func (p *Proxy) accept() {
	for {
		in, err := p.ln.Accept()
		if err != nil {
			return
		}
		out, err := net.Dial("tcp", p.target)
		if err != nil {
			_ = in.Close()
			continue
		}
		p.mu.Lock()
		p.conns = append(p.conns, in, out)
		p.mu.Unlock()
		go p.pipe(in, out)
		go p.pipe(out, in)
	}
}

func (p *Proxy) pipe(src, dst net.Conn) {
	buf := make([]byte, 32*1024)
	for {
		n, err := src.Read(buf)
		if n > 0 && !p.blackhole.Load() {
			if _, werr := dst.Write(buf[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			_ = dst.Close()
			return
		}
	}
}
