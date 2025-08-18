package redis

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

type (
	Set    map[string]*Record
	Record struct {
		Data      resp.BulkStringData
		Timeout   time.Time
		isExpired bool
	}
)

type Controller struct {
	set Set
}

func NewController() *Controller {
	return &Controller{
		set: make(Set),
	}
}

func (c *Controller) Handle(data resp.ArraysData) resp.GeneralData {
	utils.Assert(
		utils.InstanceOf[resp.BulkStringData](data.Datas[0].Data),
		"command must be a bulk string",
	)

	cmdData := data.Datas[0].Data.(resp.BulkStringData)
	switch cmd := strings.ToUpper(cmdData.Data); cmd {

	case "ECHO":
		return data.Datas[1]

	case "PING":
		return resp.GeneralData{
			Data: resp.SimpleStringData{Data: "PONG"},
		}
	case "SET":
		utils.Assert(data.Length >= 3)
		key := data.Datas[1].Data
		value := data.Datas[2].Data
		utils.Assert(
			utils.InstanceOf[resp.BulkStringData](key),
			"key must be bulk strings",
		)
		utils.Assert(
			utils.InstanceOf[resp.BulkStringData](value),
			"value must be bulk strings",
		)

		record := Record{
			Data: value.(resp.BulkStringData),
		}
		if data.Length > 3 {
			optKey := data.Datas[3].Data
			switch strings.ToLower(optKey.(resp.BulkStringData).Data) {
			case "px":
				utils.Assert(data.Length == 5)
				optValue := data.Datas[4].Data.(resp.BulkStringData).Data
				ttlInMs, err := strconv.Atoi(optValue)
				if err != nil {
					fmt.Println("invalid ttl")
				}
				record.Timeout = time.Now().Add(time.Millisecond * time.Duration(ttlInMs))
			}
		}

		fmt.Println("set key", key.RawString())
		c.set[key.RawString()] = &record
		return resp.GeneralData{
			Data: resp.SimpleStringData{Data: "OK"},
		}
	case "GET":
		utils.Assert(data.Length == 2)
		key := data.Datas[1].Data
		utils.Assert(
			utils.InstanceOf[resp.BulkStringData](key),
			"key must be bulk strings",
		)
		fmt.Println("get key", key.RawString())
		record := c.set[key.RawString()]
		if record.isExpired {
			return resp.GeneralData{
				Data: resp.NullBulkStringData{},
			}
		}
		if !record.Timeout.IsZero() &&
			record.Timeout.Before(time.Now()) {
			record.isExpired = true
			return resp.GeneralData{
				Data: resp.NullBulkStringData{},
			}
		}
		return resp.GeneralData{
			Data: record.Data,
		}
	default:
		return resp.GeneralData{
			// todo data
		}
	}
}
