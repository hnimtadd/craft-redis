package redis

import (
	"github.com/codecrafters-io/redis-starter-go/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

// command from client only contains resp.BuildStringData, so we maintain this
// struct for convenience.
type command struct {
	cmd  resp.BulkStringData
	args []resp.BulkStringData
}

// From redis docs:
// A client sends the Redis server an array consisting of only bulk strings.
func parse(data resp.ArraysData) (*command, error) {
	if data.Length < 1 {
		return nil, ErrInvalidCmd
	}
	if !utils.InstanceOf[resp.BulkStringData](data.Datas[0]) {
		return nil, ErrInvalidCmd
	}
	cmd := data.Datas[0].(resp.BulkStringData)
	args := make([]resp.BulkStringData, data.Length-1)
	for idx, arg := range data.Datas[1:] {
		if !utils.InstanceOf[resp.BulkStringData](arg) {
			return nil, ErrInvalidCmd
		}
		args[idx] = arg.(resp.BulkStringData)
	}
	return &command{
		cmd:  cmd,
		args: args,
	}, nil
}
