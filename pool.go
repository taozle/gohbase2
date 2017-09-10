package gohbase2

import "sync"

type Pool struct {
	//
	Total int

	// Whether wait if pool was exhausted.
	Wait bool

	addr string
	list chan *Conn
	mu   sync.Mutex
}

func NewPool(addr string) *Pool {
	return &Pool{
		addr: addr,
		list: make(chan *Conn, 100),
	}
}

// Get get a connection from pool if available or create new.
// return nil if
func (p *Pool) Get() *Conn {
	if p.Wait {
		return <-p.list
	}

	if v, ok := <-p.list; ok {
		return v
	} else {
		return newConn(p.addr)
	}
}

// Put put the connection into pool, abandoned if the pool is full.
// return false if the connection is abandoned.
func (p *Pool) Put(c *Conn) bool {
	select {
	case p.list <- c:
		return true
	default:
		c.Close()
		return false
	}
}
