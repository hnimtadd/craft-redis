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

type StringValue struct {
	Data      resp.BulkStringData
	Timeout   time.Time
	isExpired bool
}

type ListValue struct {
	*BLList[resp.BulkStringData]
}

func NewListValue() *ListValue {
	return &ListValue{NewBLList[resp.BulkStringData]()}
}
