package redis

import (
	"fmt"
	"math"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/dsa"
	"github.com/codecrafters-io/redis-starter-go/internal/network"
	"github.com/codecrafters-io/redis-starter-go/internal/redis/resp"
	"github.com/codecrafters-io/redis-starter-go/internal/redis/state/replication"
	"github.com/codecrafters-io/redis-starter-go/utils"
	"github.com/sirupsen/logrus"
)

type Controller struct {
	data     *dsa.Set[Value]
	logger   *logrus.Logger
	sessions *dsa.Set[Session]
	queue    *dsa.Set[Queue]

	options Options

	// Master/replica replication information
	replicationState *ReplicationState
}

func NewController(opts Options) *Controller {
	log := &logrus.Logger{
		Out:       os.Stderr,
		Formatter: new(logrus.TextFormatter),
		Hooks:     make(logrus.LevelHooks),
		Level:     logrus.DebugLevel,
	}
	rep := ReplicationState{
		Role:     opts.Role,
		replicas: dsa.NewBLSet[replication.Replica](),
	}
	if opts.Role == replication.RoleMaster {
		rep.MasterReplID = "8371b4fb1155b71f4a04d3e1bc3e18c4a990aeeb"
		rep.MasterReplOffset = 0
	}
	return &Controller{
		data:             dsa.NewBLSet[Value](),
		logger:           log,
		sessions:         dsa.NewBLSet[Session](),
		queue:            dsa.NewBLSet[Queue](),
		options:          opts,
		replicationState: &rep,
	}
}

func (c *Controller) Start() error {
	if c.replicationState.Role == replication.RoleSlave {
		c.connectToMaster()
	}
	return nil
}

func (c *Controller) connectToMaster() {
	retry := 3
	masterAddr := fmt.Sprintf("%s:%v", c.options.MasterHost, c.options.MasterPort)
	c.logger.Infof("Connecting to MASTER %s", masterAddr)
	for range retry {
		tcpConn, err := net.Dial("tcp", masterAddr)
		if err != nil {
			c.logger.Infof("Error condition on socket: %s", err)
			continue
		}
		conn := network.NewConn(tcpConn)
		parser := resp.Parser{}
		{
			pingCmd := resp.ArraysData{
				Datas: []resp.Data{resp.BulkStringData{Data: "PING"}},
			}
			respBytes, err := conn.WriteThenRead([]byte(pingCmd.String()))
			if err != nil {
				c.logger.Infof("Error condition on socket: %s", err)
				tcpConn.Close()
				continue
			}
			c.logger.Info("Master replied to PING, replication can continue...")
			cmd, _, err := parser.ParseNext(respBytes)
			if err != nil {
				c.logger.Infof("Error condition on socket: %s", err)
				tcpConn.Close()
				continue
			}
			c.logger.Debug("received", resp.Raw(cmd))
		}

		{
			replConfCm := resp.ArraysData{
				Datas: []resp.Data{
					resp.BulkStringData{Data: "REPLCONF"},
					resp.BulkStringData{Data: "listening-port"},
					resp.BulkStringData{Data: fmt.Sprint(c.options.SlavePort)},
				},
			}
			respBytes, err := conn.WriteThenRead([]byte(replConfCm.String()))
			if err != nil {
				c.logger.Infof("Error condition on socket: %s", err)
				tcpConn.Close()
				continue
			}
			c.logger.Info("Master replied to 1st REPLCONF, replication can continue...")
			cmd, _, err := parser.ParseNext(respBytes)
			if err != nil {
				c.logger.Infof("Error condition on socket: %s", err)
				tcpConn.Close()
				continue
			}
			c.logger.Debug("received", resp.Raw(cmd))
		}

		{
			replConfCmd := resp.ArraysData{
				Datas: []resp.Data{
					resp.BulkStringData{Data: "REPLCONF"},
					resp.BulkStringData{Data: "capa"},
					resp.BulkStringData{Data: "psync2"},
				},
			}
			respBytes, err := conn.WriteThenRead([]byte(replConfCmd.String()))
			if err != nil {
				c.logger.Infof("Error condition on socket: %s", err)
				tcpConn.Close()
				continue
			}
			c.logger.Info("Master replied to 2nd REPLCONF, replication can continue...")
			cmd, _, err := parser.ParseNext(respBytes)
			if err != nil {
				c.logger.Infof("Error condition on socket: %s", err)
				tcpConn.Close()
				continue
			}
			c.logger.Debug("received", resp.Raw(cmd))
		}

		{
			psyncCmd := resp.ArraysData{
				Datas: []resp.Data{
					resp.BulkStringData{Data: "PSYNC"},
					resp.BulkStringData{Data: "?"},
					resp.BulkStringData{Data: "-1"},
				},
			}
			respBytes, err := conn.WriteThenRead([]byte(psyncCmd.String()))
			if err != nil {
				c.logger.Infof("Error condition on socket: %s", err)
				tcpConn.Close()
				continue
			}
			c.logger.Info("Master replied to PSYNC, replication can continue...")
			cmd, _, err := parser.ParseNext(respBytes)
			if err != nil {
				c.logger.Infof("Error condition on socket: %s", err)
				tcpConn.Close()
				continue
			}
			c.logger.Debug("received", resp.Raw(cmd))
		}

		c.logger.Info("Handshake done")
		go c.Serve(conn, WithReplicationConnection(true))
		return
	}
}

type (
	HandlerFunc  func(command, SessionInfo) (resp.Data, *resp.SimpleErrorData)
	QueueCommand struct {
		handler HandlerFunc
		cmd     command
		session SessionInfo
	}
	Queue struct {
		commands []QueueCommand
	}
)

func (c *Controller) Handle(data resp.ArraysData, sessionInfo SessionInfo) resp.Data {
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
		handler = c.ReplicaMiddleware(handler)

	case "GET":
		handler = c.HandleGET
		handler = c.MULTIMiddleware(handler)

	case "RPUSH":
		handler = c.HandleRPUSH
		handler = c.MULTIMiddleware(handler)
		handler = c.ReplicaMiddleware(handler)
	case "LRANGE":
		handler = c.HandleLRANGE
		handler = c.MULTIMiddleware(handler)

	case "LPUSH":
		handler = c.HandleLPUSH
		handler = c.MULTIMiddleware(handler)
		handler = c.ReplicaMiddleware(handler)

	case "LLEN":
		handler = c.HandleLLEN
		handler = c.MULTIMiddleware(handler)

	case "LPOP":
		handler = c.HandleLPOP
		handler = c.MULTIMiddleware(handler)
		handler = c.ReplicaMiddleware(handler)

	case "BLPOP":
		handler = c.HandleBLPOP
		handler = c.MULTIMiddleware(handler)

	case "TYPE":
		handler = c.HandleTYPE
		handler = c.MULTIMiddleware(handler)

	case "XADD":
		handler = c.HandleXADD
		handler = c.MULTIMiddleware(handler)
		handler = c.ReplicaMiddleware(handler)

	case "XRANGE":
		handler = c.HandleXRANGE
		handler = c.MULTIMiddleware(handler)

	case "XREAD":
		handler = c.HandleXREAD
		handler = c.MULTIMiddleware(handler)

	case "INCR":
		handler = c.HandleINCR
		handler = c.MULTIMiddleware(handler)
		handler = c.ReplicaMiddleware(handler)

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

	case "PSYNC":
		handler = c.HandlePSYNC

	default:
		return resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  fmt.Sprintf("unknown command '%s'", cmd.cmd.Data),
		}
	}
	res, err := handler(*cmd, sessionInfo)
	if err != nil {
		return err
	}
	return res
}

func (c *Controller) HandleECHO(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) == 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'echo' command",
		}
	}
	return cmd.args[0], nil
}

func (c *Controller) HandlePING(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	return resp.SimpleStringData{Data: "PONG"}, nil
}

func (c *Controller) HandleSET(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'set' command",
		}
	}
	return c.handleSet(cmd.args[0], cmd.args[1], cmd.args[2:]...)
}

func (c *Controller) HandleGET(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'get' command",
		}
	}
	return c.handleGet(cmd.args[0])
}

func (c *Controller) HandleRPUSH(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'rpush' command",
		}
	}
	return c.handleRPUSH(cmd.args[0], cmd.args[1:]...)
}

func (c *Controller) HandleLRANGE(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 3 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'lrange' command",
		}
	}
	keyData := cmd.args[0]
	fromString := cmd.args[1].Data
	toString := cmd.args[2].Data
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

func (c *Controller) HandleLPUSH(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'lpush' command",
		}
	}
	return c.handleLPUSH(cmd.args[0], cmd.args[1:]...)
}

func (c *Controller) HandleLLEN(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'llen' command",
		}
	}
	return c.handleLLEN(cmd.args[0])
}

func (c *Controller) HandleLPOP(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) < 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'lpop' command",
		}
	}
	var numItem uint64 = 1
	if len(cmd.args) == 2 {
		argString := cmd.args[1].Data
		parsed, err := strconv.ParseUint(argString, 10, 64)
		if err != nil {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "value is out of range, must be positive",
			}
		}
		numItem = parsed
	}
	return c.handleLPOP(cmd.args[0], numItem)
}

func (c *Controller) HandleBLPOP(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) < 2 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'blpop' command",
		}
	}
	timeoutInSecString := cmd.args[len(cmd.args)-1].Data
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
	keys := cmd.args[0 : len(cmd.args)-1]
	return c.handleBLPOP(keys, int64(timeoutInMs))
}

func (c *Controller) HandleTYPE(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'type' command",
		}
	}
	return c.handleTYPE(cmd.args[0])
}

// HandleXADD handles stream add entry command.
// example: redis-cli XADD stream_key 1526919030474-0 temperature 36 humidity 95
func (c *Controller) HandleXADD(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) < 4 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'xadd' command",
		}
	}

	key := cmd.args[0]
	entryID := cmd.args[1]
	kvs := cmd.args[2:]
	// ensure we have valid kv cmd.args
	if len(cmd.args)%2 != 0 {
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
func (c *Controller) HandleXRANGE(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	// The sequence number doesn't need to be included in the start and end IDs
	// provided to the command. If not provided, XRANGE defaults to a sequence
	// number of 0 for the start and the maximum sequence number for the end.
	if len(cmd.args) < 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'xrange' command",
		}
	}
	key := cmd.args[0]
	var start, end InputEntryID
	var err *resp.SimpleErrorData

	if len(cmd.args) == 3 {
		start, err = parseStreamEntryID(cmd.args[1])
		if err != nil {
			startUint, err := strconv.ParseUint(cmd.args[1].Data, 10, 64)
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

		end, err = parseStreamEntryID(cmd.args[2])
		if err != nil {
			endUint, err := strconv.ParseUint(cmd.args[2].Data, 10, 64)
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
func (c *Controller) HandleXREAD(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	// we loop untils we got the "STREAMS"
	var timeoutInMs *int64
loop:
	for idx := 0; idx < len(cmd.args); idx++ {
		arg := cmd.args[idx]
		switch strings.ToUpper(arg.Data) {
		case "STREAMS":
			cmd.args = cmd.args[idx+1:]
			break loop
		case "BLOCK":
			if idx+1 >= len(cmd.args) {
				return nil, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  "wrong number of arguments for 'xread' command",
				}
			}
			blockTimeInMsString := cmd.args[idx+1].Data

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
	if len(cmd.args) < 2 || len(cmd.args)%2 != 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'xread' command",
		}
	}
	entryIDsData := cmd.args[len(cmd.args)/2:]
	keys := cmd.args[0 : len(cmd.args)/2]
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
func (c *Controller) HandleINCR(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'incr' command",
		}
	}
	return c.handleINCR(cmd.args[0])
}

// HandleMULTI handles MULTI
// example: $ redis-cli
// > MULTI
// OK
// > SET foo 41
// QUEUED
// > INCR foo
// QUEUED
func (c *Controller) HandleMULTI(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 0 {
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
func (c *Controller) HandleEXEC(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 0 {
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
func (c *Controller) HandleDISCARD(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'exec' command",
		}
	}
	return c.handleDISCARD(session)
}

func (c *Controller) HandleInfo(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) != 1 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'info' command",
		}
	}
	return c.handleINFO(cmd.args[0])
}

func (c *Controller) HandleREPLCONF(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	if len(cmd.args) == 0 {
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "wrong number of arguments for 'replconf' command",
		}
	}
	switch strings.ToLower(cmd.args[0].Data) {
	case "listening-port":
		if len(cmd.args) != 2 {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "wrong number of arguments for 'replconf' command",
			}
		}

		replicaConfig := replication.Config{}
		port, err := strconv.ParseUint(cmd.args[1].Data, 10, 64)
		if err != nil {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "value is not an integer or out of range",
			}
		}
		replicaConfig.ListeningPort = int(port)
		return c.handleREPLCONF(replicaConfig, session)
	case "capa":
		if len(cmd.args) != 2 {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "wrong number of arguments for 'replconf' command",
			}
		}

		return resp.BulkStringData{Data: "OK"}, nil

	case "getack":
		if len(cmd.args) != 2 {
			return nil, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "wrong number of arguments for 'replconf' command",
			}
		}

		return resp.ArraysData{
			Datas: []resp.Data{
				resp.BulkStringData{Data: "REPLCONF"},
				resp.BulkStringData{Data: "ACK"},
				resp.BulkStringData{Data: "0"},
			},
		}, nil

	default:
		return nil, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "unknown arguments in 'REPLCONF' command",
		}
	}
}

func (c *Controller) HandlePSYNC(cmd command, session SessionInfo) (resp.Data, *resp.SimpleErrorData) {
	return c.handlePSYNC(session)
}
