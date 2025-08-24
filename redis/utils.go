package redis

import (
	"fmt"
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

func parseStreamEntryID(entryID resp.BulkStringData) (EntryID, *resp.SimpleErrorData) {
	// early return incase we have a * entryID, that means later we need
	// to genrate both timeID part and sequence part
	if entryID.Data == "*" {
		return EntryID{
			timestampMS: -1,
			sequenceNum: -1,
		}, nil
	}

	parts := strings.Split(entryID.Data, "-")
	if len(parts) != 2 {
		return EntryID{
				timestampMS: 0,
				sequenceNum: 0,
			}, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "Invalid stream ID specified as stream command argument",
			}
	}
	timestampMSString := parts[0]
	var timestampMS int64
	switch timestampMSString {
	case "*":
		timestampMS = -1
	default:
		timestampMSUint, err := strconv.ParseUint(timestampMSString, 10, 64)
		if err != nil {
			fmt.Println("error")
			return EntryID{
					timestampMS: 0,
					sequenceNum: 0,
				}, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  "Invalid stream ID specified as stream command argument",
				}
		}
		timestampMS = int64(timestampMSUint)
	}
	sequenceNumString := parts[1]
	var sequenceNum int64
	switch sequenceNumString {
	case "*":
		sequenceNum = -1
	default:
		sequenceNumUint, err := strconv.ParseUint(sequenceNumString, 10, 64)
		if err != nil {
			return EntryID{
					timestampMS: 0,
					sequenceNum: 0,
				}, &resp.SimpleErrorData{
					Type: resp.SimpleErrorTypeGeneric,
					Msg:  "Invalid stream ID specified as stream command argument",
				}
		}
		sequenceNum = int64(sequenceNumUint)
	}
	return EntryID{
		timestampMS: timestampMS,
		sequenceNum: sequenceNum,
	}, nil
}

func fullfillStreamEntryID(stream *SetValueStream, id EntryID) (EntryID, *resp.SimpleErrorData) {
	if id.timestampMS == 0 && id.sequenceNum == 0 {
		return EntryID{timestampMS: -1, sequenceNum: -1}, &resp.SimpleErrorData{
			Type: resp.SimpleErrorTypeGeneric,
			Msg:  "The ID specified in XADD must be greater than 0-0",
		}
	}
	sequenceNum := id.sequenceNum
	timestampMS := id.timestampMS
	now := time.Now().UnixMilli()
	switch streamLength := stream.Len(); streamLength {
	case 0:
		if sequenceNum == -1 {
			if timestampMS == 0 {
				// the minimum valid ID is 0-1
				sequenceNum = 1
			} else {
				sequenceNum = 0
			}
		}
		if timestampMS == -1 {
			timestampMS = now
		}
	default:
		last := stream.At(stream.Len() - 1)

		lastTimestampMS, lastSequenceNum := last.ID.timestampMS, last.ID.sequenceNum
		// if the current top ID in the stream has a time greater than the current
		// local time of the instance, Redis uses the top entry time instead
		// and increments the sequence part of the ID.
		if timestampMS == -1 {
			timestampMS = max(now, lastTimestampMS)
		} else if timestampMS < lastTimestampMS {
			return EntryID{timestampMS: -1, sequenceNum: -1}, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "The ID specified in XADD is equal or smaller than the target stream top item",
			}
		}
		if sequenceNum == -1 {
			if timestampMS == lastTimestampMS {
				sequenceNum = lastSequenceNum + 1
			} else {
				sequenceNum = 0
			}
		}
		if timestampMS == lastTimestampMS && sequenceNum <= lastSequenceNum {
			return EntryID{timestampMS: -1, sequenceNum: -1}, &resp.SimpleErrorData{
				Type: resp.SimpleErrorTypeGeneric,
				Msg:  "The ID specified in XADD is equal or smaller than the target stream top item",
			}
		}
	}
	return EntryID{timestampMS: timestampMS, sequenceNum: sequenceNum}, nil
}
