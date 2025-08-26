package redis

import (
	"math"
	"strconv"
	"strings"
	"time"

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
func parse(data resp.ArraysData) (*command, *resp.SimpleErrorData) {
	if len(data.Datas) < 1 {
		return nil, &ErrInvalidCmd
	}
	if !utils.InstanceOf[resp.BulkStringData](data.Datas[0]) {
		return nil, &ErrInvalidCmd
	}
	cmd := data.Datas[0].(resp.BulkStringData)
	args := make([]resp.BulkStringData, len(data.Datas)-1)
	for idx, arg := range data.Datas[1:] {
		if !utils.InstanceOf[resp.BulkStringData](arg) {
			return nil, &ErrInvalidCmd
		}
		args[idx] = arg.(resp.BulkStringData)
	}
	return &command{
		cmd:  cmd,
		args: args,
	}, nil
}

func parseStreamEntryID(entryID resp.BulkStringData) (InputEntryID, *resp.SimpleErrorData) {
	// early return incase we have a * entryID, that means later we need
	// to genrate both timeID part and sequence part
	switch entryID.Data {
	case "*":
		return InputEntryID{timestampMS: nil, sequenceNum: nil, value: entryID.Data}, nil
	case "-":
		return InputEntryID{timestampMS: ptr[int64](0), sequenceNum: ptr[int64](0), value: entryID.Data}, nil
	case "+":
		return InputEntryID{timestampMS: ptr[int64](math.MaxInt64), sequenceNum: ptr[int64](math.MaxInt64), value: entryID.Data}, nil
	}

	parts := strings.Split(entryID.Data, "-")
	if len(parts) != 2 {
		return InputEntryID{}, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "Invalid stream ID specified as stream command argument",
		}
	}
	timestampMSString := parts[0]
	var timestampMS *int64
	switch timestampMSString {
	case "*":
		timestampMS = nil
	default:
		timestampMSUint, err := strconv.ParseUint(timestampMSString, 10, 64)
		if err != nil {
			return InputEntryID{}, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "Invalid stream ID specified as stream command argument",
			}
		}
		timestampMS = ptr[int64](int64(timestampMSUint))
	}
	sequenceNumString := parts[1]
	var sequenceNum *int64
	switch sequenceNumString {
	case "*":
		sequenceNum = nil
	default:
		sequenceNumUint, err := strconv.ParseUint(sequenceNumString, 10, 64)
		if err != nil {
			return InputEntryID{}, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "Invalid stream ID specified as stream command argument",
			}
		}
		sequenceNum = ptr(int64(sequenceNumUint))
	}
	return InputEntryID{timestampMS: timestampMS, sequenceNum: sequenceNum, value: entryID.Data}, nil
}

func fullfillStreamEntryID(stream *SetValueStream, id InputEntryID) (EntryID, *resp.SimpleErrorData) {
	if id.timestampMS != nil && *id.timestampMS == 0 &&
		id.sequenceNum != nil && *id.sequenceNum == 0 {
		return EntryID{}, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "The ID specified in XADD must be greater than 0-0",
		}
	}
	sequenceNum := id.sequenceNum
	timestampMS := id.timestampMS
	now := time.Now().UnixMilli()
	switch streamLength := stream.Len(); streamLength {
	case 0:
		if sequenceNum == nil {
			if timestampMS != nil && *timestampMS == 0 {
				// the minimum valid ID is 0-1
				sequenceNum = ptr[int64](1)
			} else {
				sequenceNum = ptr[int64](0)
			}
		}
		if timestampMS == nil {
			timestampMS = ptr(now)
		}
	default:
		last := stream.At(stream.Len() - 1)

		lastTimestampMS, lastSequenceNum := last.ID.timestampMS, last.ID.sequenceNum
		// if the current top ID in the stream has a time greater than the current
		// local time of the instance, Redis uses the top entry time instead
		// and increments the sequence part of the ID.
		if timestampMS == nil {
			timestampMS = ptr(max(now, lastTimestampMS))
		} else if *timestampMS < lastTimestampMS {
			return EntryID{}, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "The ID specified in XADD is equal or smaller than the target stream top item",
			}
		}
		if sequenceNum == nil {
			if *timestampMS == lastTimestampMS {
				sequenceNum = ptr(lastSequenceNum + 1)
			} else {
				sequenceNum = ptr[int64](0)
			}
		}
	}
	utils.Assert(timestampMS != nil && sequenceNum != nil)
	return EntryID{timestampMS: *timestampMS, sequenceNum: *sequenceNum}, nil
}

func ptr[T any](value T) *T {
	return &value
}
