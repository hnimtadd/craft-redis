package redis

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal/network"
	"github.com/codecrafters-io/redis-starter-go/internal/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

type (
	ServeOption  func(*serveOptions) *serveOptions
	serveOptions struct {
		IsReplicationConnection bool
	}
)

var defaultOpts = serveOptions{
	IsReplicationConnection: false,
}

func WithReplicationConnection(isReplication bool) ServeOption {
	return func(opts *serveOptions) *serveOptions {
		opts.IsReplicationConnection = isReplication
		return opts
	}
}

func (c *Controller) Serve(conn network.Connection, options ...ServeOption) {
	option := &defaultOpts
	if len(options) > 0 {
		for _, opt := range options {
			option = opt(option)
		}
	}
	c.serve(conn, *option)
}

func (c *Controller) serve(conn network.Connection, opt serveOptions) {
	innerConn := conn.Conn()
	remoteAddr := innerConn.RemoteAddr().String()
	localAdd := innerConn.LocalAddr().String()
	hasher := sha256.New()
	hashBytes := hasher.Sum([]byte(remoteAddr + localAdd))
	hash := string(hashBytes)

	c.logger.Debug("receive connection ", remoteAddr)
	session := Session{
		Hash:                 hash,
		RemoteAddr:           innerConn.RemoteAddr().String(),
		Conn:                 conn,
		IsReplicationSession: opt.IsReplicationConnection,
	}
	info := SessionInfo{
		Hash:                 hash,
		IsReplicationSession: opt.IsReplicationConnection,
	}
	c.sessions.Set(hash, &session)
	defer func() {
		c.logger.Debug("cleaning connection ", remoteAddr)
		c.sessions.Remove(hash)
		innerConn.Close()
	}()

	parser := resp.Parser{}
	for {
		utils.Assert(conn != nil)
		data := conn.Read()

		fmt.Println("receive", strconv.Quote(string(data)))
		cmd, _, err := parser.ParseNext(data)
		if err != nil {
			fmt.Println("failed to get data:", err)
			continue
		}
		switch data := cmd.(type) {
		case resp.ArraysData:
			res := c.Handle(data, info)
			if res != nil {
				utils.Assert(conn != nil)
				err := conn.Write([]byte(res.String()))
				if err != nil {
					fmt.Println("failed to write to conn", err)
					return
				}
				fmt.Println("return", resp.Raw(res))
			}

		default:
			fmt.Println("unsupported")
		}

	}
}
