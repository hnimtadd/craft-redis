package redis

import (
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

type Controller struct {
	data *Set[Value]
}

func NewController() *Controller {
	return &Controller{
		data: NewBLSet[Value](),
	}
}

func (c *Controller) Handle(data resp.ArraysData) resp.Data {
	utils.Assert(
		utils.InstanceOf[resp.BulkStringData](data.Datas[0]),
		"command must be a bulk string",
	)

	cmd, err := parse(data)
	if err != nil {
		return resp.SimpleErrorData{Msg: err.Error()}
	}

	var handler func([]resp.BulkStringData) (resp.Data, error)
	switch cmd := strings.ToUpper(cmd.cmd.Data); cmd {

	case "ECHO":
		handler = c.HandleECHO

	case "PING":
		handler = c.HandlePING

	case "SET":
		handler = c.HandleSET

	case "GET":
		handler = c.HandleGET

	case "RPUSH":
		handler = c.HandleRPUSH

	case "LRANGE":
		handler = c.HandleLRANGE

	case "LPUSH":
		handler = c.HandleLPUSH

	case "LLEN":
		handler = c.HandleLLEN

	case "LPOP":
		handler = c.HandleLPOP

	case "BLPOP":
		handler = c.HandleBLPOP

	case "TYPE":
		handler = c.HandleType

	case "XADD":
		handler = c.HandleXADD

	default:
		return resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "NOT SUPPORTED COMMAND",
			// todo data
		}
	}
	res, err := handler(cmd.args)
	if err != nil {
		return resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  err.Error(),
		}
	}
	return res
}

func (c *Controller) HandleECHO(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) == 0 {
		return nil, ErrInvalidArgs
	}
	return args[0], nil
}

func (c *Controller) HandlePING(args []resp.BulkStringData) (resp.Data, error) {
	return resp.SimpleStringData{Data: "PONG"}, nil
}

func (c *Controller) HandleSET(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) < 2 {
		return nil, ErrInvalidArgs
	}
	return c.handleSet(args[0], args[1], args[2:]...)
}

func (c *Controller) HandleGET(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) != 1 {
		return nil, ErrInvalidArgs
	}
	return c.handleGet(args[0])
}

func (c *Controller) HandleRPUSH(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) < 2 {
		return nil, ErrInvalidArgs
	}
	return c.handleRPUSH(args[0], args[1:]...)
}

func (c *Controller) HandleLRANGE(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) != 3 {
		return nil, ErrInvalidArgs
	}
	keyData := args[0]
	fromString := args[1].Data
	toString := args[2].Data
	from, err := strconv.Atoi(fromString)
	if err != nil {
		return nil, ErrInvalidArgs
	}
	to, err := strconv.Atoi(toString)
	if err != nil {
		return nil, ErrInvalidArgs
	}
	return c.handleLRANGE(keyData, from, to)
}

func (c *Controller) HandleLPUSH(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) < 2 {
		return nil, ErrInvalidArgs
	}
	return c.handleLPUSH(args[0], args[1:]...)
}

func (c *Controller) HandleLLEN(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) != 1 {
		return nil, ErrInvalidArgs
	}
	return c.handleLLEN(args[0])
}

func (c *Controller) HandleLPOP(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) < 1 {
		return nil, ErrInvalidArgs
	}
	numItem := 1
	if len(args) == 2 {
		argString := args[1].Data
		parsed, err := strconv.Atoi(argString)
		if err != nil {
			return nil, ErrInvalidArgs
		}
		if parsed < 0 {
			return nil, ErrInvalidArgs
		}
		numItem = parsed
	}
	return c.handleLPOP(args[0], numItem)
}

func (c *Controller) HandleBLPOP(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) < 2 {
		return nil, ErrInvalidArgs
	}
	timeoutInSecString := args[len(args)-1].Data
	timeoutInSec, err := strconv.ParseFloat(timeoutInSecString, 64)
	if err != nil {
		return nil, ErrInvalidArgs
	}
	timeoutInMs := timeoutInSec * 1000
	keys := args[0 : len(args)-1]
	return c.handleBLPOP(keys, int64(timeoutInMs))
}

func (c *Controller) HandleType(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) != 1 {
		return nil, ErrInvalidArgs
	}
	return c.handleTYPE(args[0])
}

// redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95
func (c *Controller) HandleXADD(args []resp.BulkStringData) (resp.Data, error) {
	if len(args) < 4 {
		return nil, ErrInvalidArgs
	}

	key := args[0]
	streamID := args[1]
	args = args[2:]
	// ensure we have valid kv args
	if len(args)%2 != 0 {
		return nil, ErrInvalidArgs
	}

	kvs := make([]StreamEntryKV, len(args)/2)
	for idx := 0; idx < len(args)/2; idx++ {
		kvs[idx] = StreamEntryKV{
			Key:   args[idx*2],
			Value: args[idx*2+1],
		}
	}
	return c.handleXADD(key, streamID, kvs)
}
