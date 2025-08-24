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
	ID  EntryID
	KVs []resp.BulkStringData
}

type EntryID struct {
	timestampMS int64
	sequenceNum int64
}

func (e EntryID) Data() resp.BulkStringData {
	return resp.BulkStringData{
		Data: fmt.Sprintf("%d-%d", e.timestampMS, e.sequenceNum),
	}
}

func (e EntryID) Cmp(o EntryID) int {
	if e.timestampMS > o.timestampMS {
		return 1
	} else if e.timestampMS < o.timestampMS {
		return -1
	}
	if e.sequenceNum > o.sequenceNum {
		return 1
	} else if e.sequenceNum < o.sequenceNum {
		return -1
	}
	return 0
}

func (e EntryID) IsZero() bool {
	return e.timestampMS == -1 && e.sequenceNum == -1
}
