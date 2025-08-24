package redis

import (
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
	ID  resp.BulkStringData
	KVs []StreamEntryKV
}

type StreamEntryKV struct {
	Key   resp.BulkStringData
	Value resp.BulkStringData
}
