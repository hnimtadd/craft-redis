package redis

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

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
func parse(data resp.ArraysData) (*command, error) {
	if data.Length < 1 {
		return nil, ErrInvalidCmd
	}
	if !utils.InstanceOf[resp.BulkStringData](data.Datas[0]) {
		return nil, ErrInvalidCmd
	}
	cmd := data.Datas[0].(resp.BulkStringData)
	args := make([]resp.BulkStringData, data.Length-1)
	for idx, arg := range data.Datas[1:] {
		if !utils.InstanceOf[resp.BulkStringData](arg) {
			return nil, ErrInvalidCmd
		}
		args[idx] = arg.(resp.BulkStringData)
	}
	return &command{
		cmd:  cmd,
		args: args,
	}, nil
}

func parseStreamEntryID(entryID resp.BulkStringData) (uint64, uint64, error) {
	parts := strings.Split(entryID.Data, "-")
	if len(parts) != 2 {
		return 0, 0, errors.New("invalid entryID")
	}
	timestampMSString := parts[0]
	sequenceNumString := parts[1]
	timestampMS, err := strconv.ParseUint(timestampMSString, 10, 64)
	if err != nil {
		return 0, 0, errors.New("invalid milliseconds time")
	}
	sequenceNum, err := strconv.ParseUint(sequenceNumString, 10, 64)
	if err != nil {
		return 0, 0, errors.New("invalid sequence number")
	}
	return timestampMS, sequenceNum, nil
}

func validteStreamEntryID(stream *SetValueStream, entryID resp.BulkStringData) error {
	timestampMS, sequenceNum, err := parseStreamEntryID(entryID)
	if err != nil {
		return err
	}
	if timestampMS == 0 && sequenceNum == 0 {
		return fmt.Errorf("The ID specified in XADD must be greater than 0-0")
	}
	if stream.Len() > 0 {
		last := stream.At(stream.Len() - 1)
		lastTimestampMS, lastSequenceNum, _ := parseStreamEntryID(last.ID)
		if timestampMS < lastTimestampMS {
			return errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		}
		if timestampMS == lastTimestampMS && sequenceNum <= lastSequenceNum {
			return errors.New("The ID specified in XADD is equal or smaller than the target stream top item")
		}
	}
	return nil
}
