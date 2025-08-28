package redis

import (
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/redis/state/replication"
	"github.com/codecrafters-io/redis-starter-go/utils"
	"github.com/sirupsen/logrus"
)

type Controller struct {
	data     *Set[Value]
	logger   *logrus.Logger
	sessions *Set[Session]
	queue    *Set[Queue]

	options Options

	// Master/replica replication information
	repState *replication.Replication
}

func NewController(opts Options) *Controller {
	log := &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}
	rep := replication.Replication{
		Role: opts.Role,
	}
	if opts.Role == replication.RoleMaster {
		rep.MasterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		rep.MasterReplOffset = 0
	}
	return &Controller{
		data:     NewBLSet[Value](),
		logger:   log,
		sessions: NewBLSet[Session](),
		queue:    NewBLSet[Queue](),
		options:  opts,
		repState: &rep,
	}
}

func (c *Controller) Start() error {
	if c.repState.Role == replication.RoleSlave {
		c.connectToMaster()
	}
	return nil
}

func (c *Controller) connectToMaster() {
	retry := 3
	masterAddr := fmt.Sprintf("%s:%v", c.options.MasterHost, c.options.MasterPort)
	c.logger.Infof("Connecting to MASTER %s", masterAddr)
	for range retry {
		err := c.Send(masterAddr, resp.ArraysData{
			Datas: []resp.Data{resp.BulkStringData{Data: "PING"}},
		})
		if err != nil {
			c.logger.Infof("Error condition on socket: %s", err)
			continue
		}
		c.logger.Info("Master replied to PING, replication can continue...")

		err = c.Send(masterAddr, resp.ArraysData{
			Datas: []resp.Data{
				resp.BulkStringData{Data: "REPLCONF"},
				resp.BulkStringData{Data: "listening-port"},
				resp.BulkStringData{Data: fmt.Sprint(c.options.SlavePort)},
			},
		})
		if err != nil {
			c.logger.Infof("Error condition on socket: %s", err)
			continue
		}
		c.logger.Info("Master replied to 1st REPLCONF, replication can continue...")

		err = c.Send(masterAddr, resp.ArraysData{
			Datas: []resp.Data{
				resp.BulkStringData{Data: "REPLCONF"},
				resp.BulkStringData{Data: "capa"},
				resp.BulkStringData{Data: "psync2"},
			},
		})
		if err != nil {
			c.logger.Infof("Error condition on socket: %s", err)
			continue
		}
		c.logger.Info("Master replied to 2nd REPLCONF, replication can continue...")
		return
	}
}

func (c *Controller) Send(addr string, data resp.Data) error {
	conn, err := net.Dial("tcp", addr)
	dataBytes := []byte(data.String())
	written, err := conn.Write(dataBytes)
	if written != len(dataBytes) {
		return fmt.Errorf("failed to write full command to connection")
	}
	return err
}

type (
	HandlerFunc func([]resp.BulkStringData, Session) (resp.Data, *resp.SimpleErrorData)
	Queue       struct {
		commands []struct {
			handler     HandlerFunc
			args        []resp.BulkStringData
			sessionInfo Session
		}
	}
)

func (c *Controller) Handle(data resp.ArraysData, sessionInfo Session) resp.Data {
	cmd, err := parse(data)
	if err != nil {
		return err
	}

	var handler HandlerFunc
	switch strings.ToUpper(cmd.cmd.Data) {

	case "ECHO":
		handler = c.HandleECHO
		handler = c.MULTIMiddleware(handler)

	case "PING":
		handler = c.HandlePING
		handler = c.MULTIMiddleware(handler)

	case "SET":
		handler = c.HandleSET
		handler = c.MULTIMiddleware(handler)

	case "GET":
		handler = c.HandleGET
		handler = c.MULTIMiddleware(handler)

	case "RPUSH":
		handler = c.HandleRPUSH
		handler = c.MULTIMiddleware(handler)

	case "LRANGE":
		handler = c.HandleLRANGE
		handler = c.MULTIMiddleware(handler)

	case "LPUSH":
		handler = c.HandleLPUSH
		handler = c.MULTIMiddleware(handler)

	case "LLEN":
		handler = c.HandleLLEN
		handler = c.MULTIMiddleware(handler)

	case "LPOP":
		handler = c.HandleLPOP
		handler = c.MULTIMiddleware(handler)

	case "BLPOP":
		handler = c.HandleBLPOP
		handler = c.MULTIMiddleware(handler)

	case "TYPE":
		handler = c.HandleTYPE
		handler = c.MULTIMiddleware(handler)

	case "XADD":
		handler = c.HandleXADD
		handler = c.MULTIMiddleware(handler)

	case "XRANGE":
		handler = c.HandleXRANGE
		handler = c.MULTIMiddleware(handler)

	case "XREAD":
		handler = c.HandleXREAD
		handler = c.MULTIMiddleware(handler)

	case "INCR":
		handler = c.HandleINCR
		handler = c.MULTIMiddleware(handler)

	case "MULTI":
		handler = c.HandleMULTI

	case "EXEC":
		handler = c.HandleEXEC

	case "DISCARD":
		handler = c.HandleDISCARD

	case "INFO":
		handler = c.HandleInfo
	case "REPLCONF":
		handler = c.HandleREPLCONF

	default:
		return resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  fmt.Sprintf("unknown command '%s'", cmd.cmd.Data),
		}
	}
	res, err := handler(cmd.args, sessionInfo)
	if err != nil {
		return err
	}
	return res
}

func (c *Controller) MULTIMiddleware(next HandlerFunc) HandlerFunc {
	return func(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
		if !c.queue.Has(session.Hash) {
			return next(args, session)
		}
		queue, _ := c.queue.Get(session.Hash)
		queue.commands = append(queue.commands, struct {
			handler     HandlerFunc
			args        []resp.BulkStringData
			sessionInfo Session
		}{
			handler:     next,
			args:        args,
			sessionInfo: session,
		})
		return resp.SimpleStringData{Data: "QUEUED"}, nil
	}
}

func (c *Controller) HandleECHO(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) == 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'echo' command",
		}
	}
	return args[0], nil
}

func (c *Controller) HandlePING(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	return resp.SimpleStringData{Data: "PONG"}, nil
}

func (c *Controller) HandleSET(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'set' command",
		}
	}
	return c.handleSet(args[0], args[1], args[2:]...)
}

func (c *Controller) HandleGET(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'get' command",
		}
	}
	return c.handleGet(args[0])
}

func (c *Controller) HandleRPUSH(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'rpush' command",
		}
	}
	return c.handleRPUSH(args[0], args[1:]...)
}

func (c *Controller) HandleLRANGE(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
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

func (c *Controller) HandleLPUSH(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'lpush' command",
		}
	}
	return c.handleLPUSH(args[0], args[1:]...)
}

func (c *Controller) HandleLLEN(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'llen' command",
		}
	}
	return c.handleLLEN(args[0])
}

func (c *Controller) HandleLPOP(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
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

func (c *Controller) HandleBLPOP(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
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

func (c *Controller) HandleTYPE(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
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
func (c *Controller) HandleXADD(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
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
func (c *Controller) HandleXRANGE(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
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
func (c *Controller) HandleXREAD(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	// we loop untils we got the "STREAMS"
	var timeoutInMs *int64
loop:
	for idx := 0; idx < len(args); idx++ {
		arg := args[idx]
		switch strings.ToUpper(arg.Data) {
		case "STREAMS":
			args = args[idx+1:]
			break loop
		case "BLOCK":
			if idx+1 >= len(args) {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  "wrong number of arguments for 'xread' command",
				}
			}
			blockTimeInMsString := args[idx+1].Data

			blockTimeUint, err := strconv.ParseUint(blockTimeInMsString, 10, 64)
			if err != nil {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  "value is out of range, must be positive",
				}
			}
			timeoutInMs = ptr(int64(blockTimeUint))
		}
	}
	// we omit 1 entry for 'STREAMS'
	if len(args) < 2 || len(args)%2 != 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'xread' command",
		}
	}
	entryIDsData := args[len(args)/2:]
	keys := args[0 : len(args)/2]
	entryIDs := make([]EntryID, len(entryIDsData))
	for idx, entryID := range entryIDsData {
		switch entryID.Data {
		case "$":
			entryIDs[idx] = EntryID{
				// we dont set the timestamp and sequenceNum as this
				// one will be lazily load later inside the handler.
				value: entryID.Data,
			}
		default:
			from, err := parseStreamEntryID(entryID)
			if err != nil {
				startUint, err := strconv.ParseUint(entryID.Data, 10, 64)
				if err != nil {
					return nil, &resp.SimpleErrorData{
						Type: resp.SimpleErrorTypeGeneric,
						Msg:  "value is out of range, must be positive",
					}
				}
				from = InputEntryID{
					timestampMS: ptr(int64(startUint)),
					sequenceNum: ptr[int64](0),
					value:       entryID.Data,
				}
			}
			utils.Assert(from.timestampMS != nil && from.sequenceNum != nil)
			entryIDs[idx] = EntryID{
				timestampMS: *from.timestampMS,
				sequenceNum: *from.sequenceNum,
				value:       from.value,
			}
		}
	}
	return c.handleXREAD(keys, entryIDs, timeoutInMs)
}

// HandleINCR handles INCR
// example: redis-cli INCR foo
func (c *Controller) HandleINCR(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'incr' command",
		}
	}
	return c.handleINCR(args[0])
}

// HandleMULTI handles MULTI
// example: $ redis-cli
// > MULTI
// OK
// > SET foo 41
// QUEUED
// > INCR foo
// QUEUED
func (c *Controller) HandleMULTI(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'incr' command",
		}
	}
	return c.handleMULTI(session)
}

// HandleEXEC handles EXEC
// example: $ redis-cli
// > MULTI
// OK
// > SET foo 41
// QUEUED
// > INCR foo
// QUEUED
// > EXEC
// 1. OK
// 2. 42
func (c *Controller) HandleEXEC(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'exec' command",
		}
	}
	return c.handleEXEC(session)
}

// HandleDISCARD handles DISCARD
// example: $ redis-cli
// > MULTI
// OK
// > SET foo 41
// QUEUED
// > INCR foo
// QUEUED
// > DISCARD
// OK
// . 42
func (c *Controller) HandleDISCARD(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'exec' command",
		}
	}
	return c.handleDISCARD(session)
}

func (c *Controller) HandleInfo(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	if len(args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'info' command",
		}
	}
	return c.handleINFO(args[0])
}

func (c *Controller) HandleREPLCONF(args []resp.BulkStringData, session Session) (resp.Data, *resp.SimpleErrorData) {
	return resp.BulkStringData{Data: "OK"}, nil
}
