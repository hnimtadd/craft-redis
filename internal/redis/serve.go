package redis

import (
	"crypto/sha256"
	"fmt"
	"io"
	"net"
	"strconv"

	"github.com/codecrafters-io/redis-starter-go/internal/redis/resp"
)

func (c *Controller) Serve(conn net.Conn) {
	remoteAddr := conn.RemoteAddr().String()
	localAdd := conn.LocalAddr().String()
	hasher := sha256.New()
	hashBytes := hasher.Sum([]byte(remoteAddr + localAdd))
	hash := string(hashBytes)

	c.logger.Debug("receive connection ", remoteAddr)
	session := Session{
		Hash:       hash,
		RemoteAddr: conn.RemoteAddr().String(),
		Conn:       conn,
	}
	c.sessions.Set(hash, &session)
	defer func() {
		c.logger.Debug("cleaning connection ", remoteAddr)
		c.sessions.Remove(hash)
		conn.Close()
	}()

	parser := resp.Parser{}
	for {
		buf := make([]byte, 1024)
		n, err := conn.Read(buf)
		if err != nil {
			if err == io.EOF {
				return
			}
			fmt.Println("failed to read from conn", err)
			return
		}
		data := buf[:n]
		fmt.Println("receive", strconv.Quote(string(data)))
		cmd, _, err := parser.ParseNext(data)
		if err != nil {
			fmt.Println("failed to get data:", err)
			continue
		}
		switch data := cmd.(type) {
		case resp.ArraysData:
			res := c.Handle(data, session)

			_, err := conn.Write([]byte(res.String()))
			if err != nil {
				fmt.Println("failed to write to conn", err)
				return
			}
			fmt.Println("return", resp.Raw(res))
		default:
			fmt.Println("unsupported")
		}

	}
}
