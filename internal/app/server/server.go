package server

import (
	"fmt"
	"net"

	"github.com/codecrafters-io/redis-starter-go/internal/redis"
	"github.com/sirupsen/logrus"
)

type Options struct {
	Port int
}

type Server struct {
	opts    Options
	handler *redis.Controller
	logger  *logrus.Logger
}

func NewServer(handler *redis.Controller, opts Options) *Server {
	return &Server{
		handler: handler,
		opts:    opts,
		logger:  logrus.New(),
	}
}

func (s *Server) ListenAndServe() error {
	addr := fmt.Sprintf("0.0.0.0:%d", s.opts.Port)
	s.logger.Info("listening at ", addr)
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("failed to bind to port %d", s.opts.Port)
	}
	if err := s.handler.Start(); err != nil {
		return fmt.Errorf("failed to start controller: v", err)
	}

	s.logger.Info("Ready to accept connections tcp")

	conns := make(chan net.Conn)
	go func() {
		for {
			conn, err := l.Accept()
			if err != nil {
				fmt.Println("Error accepting connection: ", err.Error())
				return
			}
			conns <- conn
		}
	}()
	for conn := range conns {
		go s.handler.Serve(conn)
	}
	return net.ErrClosed
}
