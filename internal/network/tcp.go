package network

import (
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/codecrafters-io/redis-starter-go/internal/dsa"
)

type Connection interface {
	Conn() net.Conn
	// WriteThenRead synchronously writes data to the connection and wait for response
	// write can be made to time out and return an error after a fixed
	// time limit; see setdeadline and setwritedeadline.
	WriteThenRead(b []byte) (response []byte, err error)

	// Write synchronously writes data to the connection
	// write can be made to time out and return an error after a fixed
	// time limit; see setdeadline and setwritedeadline.
	Write(b []byte) (err error)

	// Read reads data from the connection.
	// Read can be made to time out and return an error after a fixed
	// time limit; see SetDeadline and SetReadDeadline.
	Read() (b []byte)
}

type connectionImpl struct {
	conn net.Conn

	mu      sync.RWMutex
	readCh  *dsa.BLList[chan []byte] // Single list: prepend for LIFO, append for FIFO
	dataCh  chan []byte              // Internal buffered channel for incoming data
	closeCh chan struct{}            // Signal to stop goroutines
}

// AsyncWrite implements Connection.
func (c *connectionImpl) Write(b []byte) (err error) {
	n, err := c.conn.Write(b)
	if err != nil {
		return err
	}
	if n != len(b) {
		return fmt.Errorf("expected %d to be written, actual: %d", len(b), n)
	}
	return nil
}

// Conn implements Connection.
func (c *connectionImpl) Conn() net.Conn {
	return c.conn
}

// WriteThenRead implements Connection.
func (c *connectionImpl) WriteThenRead(b []byte) (response []byte, err error) {
	c.mu.Lock()
	n, err := c.conn.Write(b)
	c.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if n != len(b) {
		return nil, fmt.Errorf("expected %d to be written, actual: %d", len(b), n)
	}

	ch := make(chan []byte, 1)
	defer close(ch)
	c.mu.Lock()
	_ = c.readCh.Prepend(ch) // LIFO: prepend to beginning, take from first
	c.mu.Unlock()
	return <-ch, nil
}

// Read implements Connection.
func (c *connectionImpl) Read() (resp []byte) {
	ch := make(chan []byte, 1)
	defer close(ch)
	c.mu.Lock()
	_ = c.readCh.Append(ch) // FIFO: append to end, take from beginning
	c.mu.Unlock()

	return <-ch
}

func (c *connectionImpl) readLoop() {
	defer close(c.dataCh)
	defer close(c.closeCh)

	for {
		buff := make([]byte, 4096)
		n, err := c.conn.Read(buff)
		if err == io.EOF {
			break
		}
		data := buff[:n]

		// Send to internal channel - blocks if buffer is full
		select {
		case c.dataCh <- data:
		case <-c.closeCh:
			return
		}
	}
}

func (c *connectionImpl) dispatcher() {
	for {
		select {
		case data, ok := <-c.dataCh:
			if !ok {
				return // dataCh closed, readLoop stopped
			}

			// Block until we have a waiter for this data
			var ch *chan []byte
			for ch == nil {
				c.mu.Lock()
				ch = c.readCh.First() // Always take from first
				if ch != nil {
					c.readCh.Remove(0) // Remove first element
					c.mu.Unlock()
					break
				}
				c.mu.Unlock()
				// No waiter yet, sleep briefly to avoid busy-waiting
				time.Sleep(10 * time.Microsecond)
			}

			*ch <- data // Send data to waiter

		case <-c.closeCh:
			return
		}
	}
}

func NewConn(conn net.Conn) Connection {
	con := &connectionImpl{
		conn:    conn,
		readCh:  dsa.NewBLList[chan []byte](),
		dataCh:  make(chan []byte, 100), // Buffered channel for incoming data
		closeCh: make(chan struct{}),
	}
	go con.readLoop()
	go con.dispatcher()
	return con
}
