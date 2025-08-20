package redis

import (
	"errors"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

type (
	Set    map[string]*Record
	List   map[string][]resp.Data
	Record struct {
		Data      resp.BulkStringData
		Timeout   time.Time
		isExpired bool
	}
)

type Controller struct {
	set  Set
	list List
}

func NewController() *Controller {
	return &Controller{
		set:  make(Set),
		list: make(List),
	}
}

func (c *Controller) HandleECHO(cmd resp.ArraysData) (resp.Data, error) {
	return cmd.Datas[1], nil
}

func (c *Controller) HandlePING(cmd resp.ArraysData) (resp.Data, error) {
	return resp.SimpleStringData{Data: "PONG"}, nil
}

func (c *Controller) HandleSET(cmd resp.ArraysData) (resp.Data, error) {
	utils.Assert(cmd.Length >= 3)
	if cmd.Length < 3 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	value := cmd.Datas[2]
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
	if cmd.Length > 3 {
		for keyIdx := 3; keyIdx < cmd.Length; keyIdx += 2 {
			optKey := cmd.Datas[keyIdx]

			utils.Assert(
				utils.InstanceOf[resp.BulkStringData](optKey),
				"option must be bulk strings",
			)
			switch strings.ToLower(optKey.(resp.BulkStringData).Data) {
			case "px":
				valIdx := keyIdx + 1
				// valIdx is 0-based
				if valIdx >= cmd.Length {
					return nil, ErrInvalidArgs
				}
				optVal := cmd.Datas[valIdx]
				utils.Assert(
					utils.InstanceOf[resp.BulkStringData](optVal),
					"option must be bulk strings",
				)
				ttlInMs, err := strconv.Atoi(optVal.(resp.BulkStringData).Data)
				if err != nil {
					return nil, errors.New("invalid ttl")
				}
				record.Timeout = time.Now().Add(time.Millisecond * time.Duration(ttlInMs))
			}
		}
	}

	c.set[resp.Raw(key)] = &record
	return resp.SimpleStringData{Data: "OK"}, nil
}

func (c *Controller) HandleGET(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length != 2 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	utils.Assert(
		utils.InstanceOf[resp.BulkStringData](key),
		"key must be bulk strings",
	)
	record, found := c.set[resp.Raw(key)]
	if !found {
		return resp.NullBulkStringData{}, nil
	}
	if record.isExpired {
		return resp.NullBulkStringData{}, nil
	}
	if !record.Timeout.IsZero() &&
		record.Timeout.Before(time.Now()) {
		record.isExpired = true
		return resp.NullBulkStringData{}, nil
	}
	return record.Data, nil
}

func (c *Controller) HandleRPUSH(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length < 3 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	lst, found := c.list[resp.Raw(key)]
	if !found {
		c.list[resp.Raw(key)] = []resp.Data{}
		lst = c.list[resp.Raw(key)]
	}
	lst = append(lst, cmd.Datas[2:]...)
	c.list[resp.Raw(key)] = lst
	return resp.Integer{Data: len(lst)}, nil
}

func (c *Controller) HandleLRANGE(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length != 4 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	utils.Assert(
		utils.InstanceOf[resp.BulkStringData](key),
		"startIndex must be a bulk string",
	)
	lst, found := c.list[resp.Raw(key)]
	if !found {
		return resp.ArraysData{}, nil
	}
	startData := cmd.Datas[2]
	if !utils.InstanceOf[resp.BulkStringData](startData) {
		return nil, ErrInvalidArgs
	}
	endData := cmd.Datas[3]
	if !utils.InstanceOf[resp.BulkStringData](endData) {
		return nil, ErrInvalidArgs
	}
	startString := startData.(resp.BulkStringData).Data
	endString := endData.(resp.BulkStringData).Data
	start, err := strconv.Atoi(startString)
	if err != nil {
		return nil, ErrInvalidArgs
	}
	end, err := strconv.Atoi(endString)
	if err != nil {
		return nil, ErrInvalidArgs
	}
	var (
		startIdx = start
		endIdx   = end
	)

	if startIdx < 0 {
		startIdx = len(lst) + startIdx
		startIdx = max(startIdx, 0)
	}
	if endIdx < 0 {
		endIdx = len(lst) + endIdx
		endIdx = max(endIdx, 0)
	}

	if startIdx > endIdx {
		return resp.ArraysData{}, nil
	}

	if startIdx > len(lst) {
		return resp.ArraysData{}, nil
	}

	endIdx = min(endIdx, len(lst)-1)
	return resp.ArraysData{
		Length: endIdx - startIdx + 1,
		Datas:  lst[startIdx : endIdx+1],
	}, nil
}

func (c *Controller) HandleLPUSH(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length < 3 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	lst, found := c.list[resp.Raw(key)]
	if !found {
		c.list[resp.Raw(key)] = []resp.Data{}
		lst = c.list[resp.Raw(key)]
	}
	// reverse so we have a list that should be exists after we add to the list
	// then we just simply append the original list.
	//
	// NOTE: this changes order of cmd Datas, in the future, if something wrong
	// with cmd.Data, check if this one is related first.
	slices.Reverse(cmd.Datas[2:])
	lst = append(cmd.Datas[2:], lst...)
	c.list[resp.Raw(key)] = lst
	return resp.Integer{Data: len(lst)}, nil
}

func (c *Controller) HandleLLEN(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length != 2 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	lst, found := c.list[resp.Raw(key)]
	if !found {
		return resp.Integer{}, nil
	}
	return resp.Integer{Data: len(lst)}, nil
}

func (c *Controller) Handle(data resp.ArraysData) resp.Data {
	utils.Assert(
		utils.InstanceOf[resp.BulkStringData](data.Datas[0]),
		"command must be a bulk string",
	)

	cmdData := data.Datas[0].(resp.BulkStringData)
	var handler func(resp.ArraysData) (resp.Data, error)
	switch cmd := strings.ToUpper(cmdData.Data); cmd {

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

	default:
		return resp.SimpleErrorData{
			Msg: "NOT SUPPORTED COMMAND",
			// todo data
		}
	}
	res, err := handler(data)
	if err != nil {
		return resp.SimpleErrorData{Msg: err.Error()}
	}
	return res
}
