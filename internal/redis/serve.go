package redis

import (
	"crypto/sha256"
	"fmt"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal/network"
	"github.com/codecrafters-io/redis-starter-go/internal/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

func (c *Controller) Serve(conn network.Connection) {
	c.serve(conn, false)
}

func (c *Controller) ServeMasterConnection(conn network.Connection) {
	c.serve(conn, true)
}

func (c *Controller) serve(conn network.Connection, isMasterConnection bool) {
	innerConn := conn.Conn()
	remoteAddr := innerConn.RemoteAddr().String()
	localAdd := innerConn.LocalAddr().String()
	hasher := sha256.New()
	hashBytes := hasher.Sum([]byte(remoteAddr + localAdd))
	hash := string(hashBytes)

	c.logger.Debug("receive connection ", remoteAddr)
	session := Session{
		Hash:               hash,
		RemoteAddr:         innerConn.RemoteAddr().String(),
		Conn:               conn,
		IsMasterConnection: isMasterConnection,
	}
	info := SessionInfo{
		Hash:               hash,
		IsMasterConnection: isMasterConnection,
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

			// Only send response if this is NOT a master connection
			// (replicas don't respond to propagated commands from master)
			if !isMasterConnection {
				utils.Assert(conn != nil)
				err := conn.Write([]byte(res.String()))
				if err != nil {
					fmt.Println("failed to write to conn", err)
					return
				}
				fmt.Println("return", resp.Raw(res))
			} else {
				fmt.Println("processed silently (master connection)")
			}
		default:
			fmt.Println("unsupported")
		}

	}
}
