package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
)

type (
	List   map[string][]resp.Data
	Record struct {
		Data      resp.BulkStringData
		Timeout   time.Time
		isExpired bool
	}
)

type Controller struct {
	set  *Set[Record]
	list *Set[BLList[resp.Data]]
}

func NewController() *Controller {
	return &Controller{
		set:  NewBLSet[Record](nil),
		list: NewBLSet(NewBLList[resp.Data]),
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

	c.set.BLSet(resp.Raw(key), &record)
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
	record, found := c.set.Get(resp.Raw(key))
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
	lst, _ := c.list.Get(resp.Raw(key))
	defer lst.Signal()
	len := lst.Append(cmd.Datas[2:]...)
	return resp.Integer{Data: len}, nil
}

func (c *Controller) HandleLRANGE(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length != 4 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	utils.Assert(
		utils.InstanceOf[resp.BulkStringData](key),
		"key must be a bulk string",
	)
	if !c.list.Has(resp.Raw(key)) {
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

	lst, _ := c.list.Get(resp.Raw(key))
	var (
		startIdx = start
		endIdx   = end
	)

	if startIdx < 0 {
		startIdx = lst.Len() + startIdx
		startIdx = max(startIdx, 0)
	}
	if endIdx < 0 {
		endIdx = lst.Len() + endIdx
		endIdx = max(endIdx, 0)
	}

	if startIdx > endIdx {
		return resp.ArraysData{}, nil
	}

	if startIdx > lst.Len() {
		return resp.ArraysData{}, nil
	}

	endIdx = min(endIdx, lst.Len()-1)
	fmt.Println("startIdx", startIdx, "endIdx", endIdx)
	eles, err := lst.Slice(uint(startIdx), uint(endIdx+1))
	if err != nil {
		return nil, fmt.Errorf("cannot get element: %v", err)
	}
	results := make([]resp.Data, len(eles))
	for idx, ele := range eles {
		results[idx] = *ele
	}
	return resp.ArraysData{
		Length: endIdx - startIdx + 1,
		Datas:  results,
	}, nil
}

func (c *Controller) HandleLPUSH(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length < 3 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	lst, _ := c.list.Get(resp.Raw(key))
	defer lst.Signal()
	// reverse so we have a list that should be exists after we add to the list
	// then we just simply append the original list.
	for _, data := range cmd.Datas[2:] {
		lst.Prepend(data)
	}

	return resp.Integer{Data: lst.Len()}, nil
}

func (c *Controller) HandleLLEN(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length != 2 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	if !c.list.Has(resp.Raw(key)) {
		return resp.Integer{}, nil
	}
	lst, _ := c.list.Get(resp.Raw(key))
	return resp.Integer{Data: lst.Len()}, nil
}

func (c *Controller) HandleLPOP(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length < 2 {
		return nil, ErrInvalidArgs
	}
	key := cmd.Datas[1]
	if !c.list.Has(resp.Raw(key)) {
		return resp.BulkStringData{}, nil
	}
	lst, _ := c.list.Get(resp.Raw(key))
	if lst.Len() == 0 {
		return resp.BulkStringData{}, nil
	}

	numItem := 1
	if cmd.Length == 3 {
		arg := cmd.Datas[2]
		if !utils.InstanceOf[resp.BulkStringData](arg) {
			return nil, ErrInvalidArgs
		}
		argString := arg.(resp.BulkStringData).Data
		parsed, err := strconv.Atoi(argString)
		if err != nil {
			return nil, ErrInvalidArgs
		}
		if parsed < 0 {
			return nil, ErrInvalidArgs
		}
		numItem = parsed
	}

	switch numItem {
	case 1:
		popItem, err := lst.Remove(0)
		if err != nil {
			return nil, err
		}
		return *popItem, nil
	default:
		numItem = min(numItem, lst.Len())
		popItems := make([]resp.Data, numItem)

		for idx := range numItem {
			popItem, err := lst.Remove(0)
			if err != nil {
				return nil, err
			}
			popItems[idx] = *popItem
		}

		return resp.ArraysData{
			Length: numItem,
			Datas:  popItems,
		}, nil
	}
}

func (c *Controller) HandleBLPOP(cmd resp.ArraysData) (resp.Data, error) {
	if cmd.Length < 3 {
		return nil, ErrInvalidArgs
	}
	timeoutInSecData := cmd.Datas[len(cmd.Datas)-1]
	if !utils.InstanceOf[resp.BulkStringData](timeoutInSecData) {
		return nil, ErrInvalidArgs
	}
	timeoutInSec, err := strconv.Atoi(timeoutInSecData.(resp.BulkStringData).Data)
	if err != nil {
		return nil, ErrInvalidArgs
	}
	keysData := cmd.Datas[1 : len(cmd.Datas)-1]
	keys := make([]resp.Data, len(keysData))
	for idx, data := range keysData {
		if !utils.InstanceOf[resp.BulkStringData](data) {
			return nil, ErrInvalidArgs
		}
		keys[idx] = data
	}

	cancelCh := make(chan any)
	doneCh := make(chan resp.Data, 1)

	for _, key := range keys {
		go func(key resp.Data) {
			lst, _ := c.list.Get(resp.Raw(key))
			if lst.Len() > 0 {
				select {
				case doneCh <- key:
					fmt.Printf("Routine for %s sent signal.\n", resp.Raw(key))
				case <-cancelCh:
					fmt.Printf("Routine for %s found another signal was already sent and was canceled.\n", resp.Raw(key))
				}
			}
			sub := lst.NewSubscription()
			select {
			case <-cancelCh:
				fmt.Printf("Routine for %s was canceled before it was signaled.\n", resp.Raw(key))
				return // Exit gracefully.
			default:
				// Not canceled yet, proceed to wait.
			}

			sub = lst.Subscribe(sub)
			waitCh := make(chan struct{})
			go func() {
				sub.Wait()
				close(waitCh)
			}()

			select {
			case <-waitCh:
				fmt.Printf("Routine for %s has been woken up.\n", resp.Raw(key))
				// Try to send on the done channel.
				select {
				case doneCh <- key:
					fmt.Printf("Routine for %s sent signal.\n", resp.Raw(key))
				case <-cancelCh:
					sub.cond.Signal()
					// If a cancel signal arrived while we were trying to send,
					// it means another routine won the race.
					fmt.Printf("Routine for %s found another signal was already sent and was canceled.\n", resp.Raw(key))
				}
			case <-cancelCh:
				sub.cond.Signal()
				fmt.Printf("Routine for %s was canceled while waiting.\n", resp.Raw(key))
				// At this point, the goroutine from `cond.Wait()` is still blocked.
				// There is no clean way to unblock a `cond.Wait()` from outside.
				// This is why the cleanup method is critical for the `A` struct to work.
			}
			sub.Deactivate()
		}(key)
	}
	defer close(cancelCh)

	if timeoutInSec > 0 {
		select {
		case key := <-doneCh:
			lst, _ := c.list.Get(resp.Raw(key))
			ele, err := lst.Remove(0)
			if err != nil {
				return nil, err
			}
			return resp.ArraysData{
				Length: 2,
				Datas:  []resp.Data{key, *ele},
			}, nil
		case <-time.After(time.Duration(timeoutInSec) * time.Second):
			return resp.NullBulkStringData{}, nil
		}
	} else {
		key := <-doneCh
		lst, _ := c.list.Get(resp.Raw(key))
		ele, err := lst.Remove(0)
		if err != nil {
			return nil, err
		}
		return resp.ArraysData{
			Length: 2,
			Datas:  []resp.Data{key, *ele},
		}, nil
	}
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

	case "LPOP":
		handler = c.HandleLPOP

	case "BLPOP":
		handler = c.HandleBLPOP

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
