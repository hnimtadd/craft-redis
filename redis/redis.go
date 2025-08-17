package redis

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

type Controller struct {
	lookup map[string]resp.BulkStringData
}

func NewController() *Controller {
	return &Controller{
		lookup: map[string]resp.BulkStringData{},
	}
}

func (c *Controller) Handle(data resp.ArraysData) resp.GeneralData {
	utils.Assert(
		data.Datas[0].Type == resp.TypeBulkString,
		"command must be a bulk string",
	)

	cmdData := data.Datas[0].Data.(resp.BulkStringData)
	switch cmd := strings.ToUpper(cmdData.Data); cmd {
	case "ECHO":
		return data.Datas[1]
	case "PING":
		return resp.GeneralData{
			Type: resp.TypeSimpleString,
			Data: resp.SimpleStringData{Data: "PONG"},
		}
	case "SET":
		utils.Assert(data.Length == 3)
		key := data.Datas[1]
		value := data.Datas[2]
		utils.Assert(key.Type == resp.TypeBulkString, "key must be bulk strings")
		utils.Assert(value.Type == resp.TypeBulkString, "value must be bulk strings")
		c.lookup[strconv.Quote(key.String())] = value.Data.(resp.BulkStringData)
		return resp.GeneralData{
			Type: resp.TypeSimpleString,
			Data: resp.SimpleStringData{Data: "OK"},
		}
	case "GET":
		utils.Assert(data.Length == 2)
		key := data.Datas[1]
		utils.Assert(key.Type == resp.TypeBulkString, "key must be bulk strings")
		value, exists := c.lookup[strconv.Quote(key.String())]
		if exists {
			fmt.Println("key", key, "found")
		}
		return resp.GeneralData{
			Type: resp.TypeBulkString,
			Data: value,
		}
	default:
		return resp.GeneralData{
			Type: resp.TypeSimpleError,
			// todo data
		}
	}
}
