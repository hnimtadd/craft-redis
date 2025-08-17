package main

import (
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
)

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	l, err := net.Listen("tcp", "0.0.0.0:6379")
	if err != nil {
		fmt.Println("Failed to bind to port 6379")
		os.Exit(1)
	}
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
		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()

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
		switch data := cmd.Data.(type) {
		case resp.ArraysData:
			if data.Datas[0].Type == resp.TypeBulkString {
				cmdData := data.Datas[0].Data.(resp.BulkStringData)
				switch cmd := strings.ToUpper(cmdData.Data); cmd {
				case "ECHO":

					_, err := conn.Write([]byte(data.Datas[1].String()))
					if err != nil {
						fmt.Println("failed to write to conn", err)
						return
					}
				case "PING":
					pongData := resp.SimpleStringData{Data: "PONG"}
					_, err := conn.Write([]byte(pongData.String()))
					if err != nil {
						fmt.Println("failed to write to conn", err)
						return
					}
				}
			}
		default:
			fmt.Println("unsupported")
		}

	}
}
