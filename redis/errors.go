package redis

import (
	"github.com/codecrafters-io/redis-starter-go/redis/resp"
)

var (
	ErrInvalidArgs = resp.SimpleErrorData{
		Type: resp.SimpleErrorTypeGeneric,
		Msg:  "invalid args",
	}
	ErrInvalidCmd = resp.SimpleErrorData{
		Type: resp.SimpleErrorTypeGeneric,
		Msg:  "invalid cmd",
	}
)
