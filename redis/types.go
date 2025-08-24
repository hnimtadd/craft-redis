package redis

import (
	"fmt"
	"time"

	"github.com/codecrafters-io/redis-starter-go/redis/resp"
)

type (
	SetValueType    string
	SetValuePayload any
	Value           struct {
		Type SetValueType
		Data SetValuePayload
	}
)

type (
	SetValueString struct {
		Data      resp.BulkStringData
		Timeout   time.Time
		isExpired bool
	}
	SetValueList struct {
		*BLList[resp.BulkStringData]
	}
	SetValueStream struct {
		*BLList[StreamEntry]
	}
)

func NewListValue() *SetValueList {
	return &SetValueList{NewBLList[resp.BulkStringData]()}
}

func NewStreamValue() *SetValueStream {
	return &SetValueStream{NewBLList[StreamEntry]()}
}

type StreamEntry struct {
	timestampMs int64
	sequenceNum int64
	KVs         []resp.BulkStringData
}

func (e StreamEntry) ID() resp.BulkStringData {
	return resp.BulkStringData{
		Data: fmt.Sprintf("%d-%d", e.timestampMs, e.sequenceNum),
	}
}
