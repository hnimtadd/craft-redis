package redis

import (
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/utils"
	"github.com/sirupsen/logrus"
)

type Controller struct {
	data   *Set[Value]
	logger *logrus.Logger
}

func NewController() *Controller {
	log := &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}
	return &Controller{
		data:   NewBLSet[Value](),
		logger: log,
	}
}

func (c *Controller) Handle(data resp.ArraysData) resp.Data {
	cmd, err := parse(data)
	if err != nil {
		return err
	}

	var handler func([]resp.BulkStringData) (resp.Data, *resp.SimpleErrorData)
	switch strings.ToUpper(cmd.cmd.Data) {

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
		handler = c.HandleTYPE

	case "XADD":
		handler = c.HandleXADD

	case "XRANGE":
		handler = c.HandleXRANGE

	case "XREAD":
		handler = c.HandleXREAD

	default:
		return resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  fmt.Sprintf("unknown command '%s'", cmd.cmd.Data),
		}
	}
	res, err := handler(cmd.args)
	if err != nil {
		return err
	}
	return res
}

func (c *Controller) HandleECHO(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) == 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'echo' command",
		}
	}
	return args[0], nil
}

func (c *Controller) HandlePING(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	return resp.SimpleStringData{Data: "PONG"}, nil
}

func (c *Controller) HandleSET(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'set' command",
		}
	}
	return c.handleSet(args[0], args[1], args[2:]...)
}

func (c *Controller) HandleGET(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'get' command",
		}
	}
	return c.handleGet(args[0])
}

func (c *Controller) HandleRPUSH(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'rpush' command",
		}
	}
	return c.handleRPUSH(args[0], args[1:]...)
}

func (c *Controller) HandleLRANGE(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 3 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'lrange' command",
		}
	}
	keyData := args[0]
	fromString := args[1].Data
	toString := args[2].Data
	from, err := strconv.ParseInt(fromString, 10, 32)
	if err != nil {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "value is not an integer or out of range",
		}
	}
	to, err := strconv.ParseInt(toString, 10, 32)
	if err != nil {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "value is not an integer or out of range",
		}
	}
	return c.handleLRANGE(keyData, int(from), int(to))
}

func (c *Controller) HandleLPUSH(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'lpush' command",
		}
	}
	return c.handleLPUSH(args[0], args[1:]...)
}

func (c *Controller) HandleLLEN(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'llen' command",
		}
	}
	return c.handleLLEN(args[0])
}

func (c *Controller) HandleLPOP(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'lpop' command",
		}
	}
	var numItem uint64 = 1
	if len(args) == 2 {
		argString := args[1].Data
		parsed, err := strconv.ParseUint(argString, 10, 64)
		if err != nil {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "value is out of range, must be positive",
			}
		}
		numItem = parsed
	}
	return c.handleLPOP(args[0], numItem)
}

func (c *Controller) HandleBLPOP(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'blpop' command",
		}
	}
	timeoutInSecString := args[len(args)-1].Data
	timeoutInSec, err := strconv.ParseFloat(timeoutInSecString, 64)
	if err != nil {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "timeout is not a float or out of range",
		}
	}
	if timeoutInSec < 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "timeout is negative",
		}
	}
	timeoutInMs := timeoutInSec * 1000
	keys := args[0 : len(args)-1]
	return c.handleBLPOP(keys, int64(timeoutInMs))
}

func (c *Controller) HandleTYPE(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'type' command",
		}
	}
	return c.handleTYPE(args[0])
}

// HandleXADD handles stream add entry command.
// example: redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95
func (c *Controller) HandleXADD(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 4 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'xadd' command",
		}
	}

	key := args[0]
	entryID := args[1]
	kvs := args[2:]
	// ensure we have valid kv args
	if len(args)%2 != 0 {
		return nil, &ErrInvalidArgs
	}

	id, err := parseStreamEntryID(entryID)
	if err != nil {
		return resp.BulkStringData{}, err
	}

	return c.handleXADD(key, id, kvs)
}

// HandleXRANGE handles querying data from a stream.
// example: redis-cli XRANGE stream_key 1526985054069 1526985054079
func (c *Controller) HandleXRANGE(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	// The sequence number doesn't need to be included in the start and end IDs
	// provided to the command. If not provided, XRANGE defaults to a sequence
	// number of 0 for the start and the maximum sequence number for the end.
	if len(args) < 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'xrange' command",
		}
	}
	key := args[0]
	var start, end InputEntryID
	var err *resp.SimpleErrorData

	if len(args) == 3 {
		start, err = parseStreamEntryID(args[1])
		if err != nil {
			startUint, err := strconv.ParseUint(args[1].Data, 10, 64)
			if err != nil {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  "value is out of range, must be positive",
				}
			}
			start = InputEntryID{
				timestampMS: ptr(int64(startUint)),
				sequenceNum: ptr[int64](0),
			}
		}

		end, err = parseStreamEntryID(args[2])
		if err != nil {
			endUint, err := strconv.ParseUint(args[2].Data, 10, 64)
			if err != nil {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  "value is out of range, must be positive",
				}
			}
			end = InputEntryID{
				timestampMS: ptr(int64(endUint)),
				sequenceNum: ptr[int64](math.MaxInt64),
			}
		}
	}
	utils.Assert(start.timestampMS != nil && start.sequenceNum != nil)
	utils.Assert(end.timestampMS != nil && end.sequenceNum != nil)
	return c.handleXRANGE(key,
		EntryID{timestampMS: *start.timestampMS, sequenceNum: *start.sequenceNum},
		EntryID{timestampMS: *end.timestampMS, sequenceNum: *end.sequenceNum},
	)
}

// HandleXREAD handles XREAD
// example: redis-cli XREAD streams some_key 1526985054069-0
func (c *Controller) HandleXREAD(args []resp.BulkStringData) (resp.Data, *resp.SimpleErrorData) {
	// we omit 1 entry for 'STREAMS'
	if len(args) < 3 || (len(args)-1)%2 != 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'xread' command",
		}
	}
	args = args[1:]
	entryIDsData := args[len(args)/2:]
	keys := args[0 : len(args)/2]
	entryIDs := make([]EntryID, len(entryIDsData))
	for idx, entryID := range entryIDsData {
		from, err := parseStreamEntryID(entryID)
		if err != nil {
			startUint, err := strconv.ParseUint(args[1].Data, 10, 64)
			if err != nil {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  "value is out of range, must be positive",
				}
			}
			from = InputEntryID{
				timestampMS: ptr(int64(startUint)),
				sequenceNum: ptr[int64](0),
			}
		}
		utils.Assert(from.timestampMS != nil && from.sequenceNum != nil)
		entryIDs[idx] = EntryID{
			timestampMS: *from.timestampMS,
			sequenceNum: *from.sequenceNum,
		}
	}
	return c.handleXREAD(keys, entryIDs)
}
