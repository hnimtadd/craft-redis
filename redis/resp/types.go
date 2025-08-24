package resp

import (
	"fmt"
	"slices"
	"strconv"
	"strings"
)

type (
	Data interface {
		String() string
	}
	SimpleStringData struct {
		Data string
	}
	BulkStringData struct {
		Length int
		Data   string
	}
	NullBulkStringData struct{}
	ArraysData         struct {
		Length int
		Datas  []Data
	}
	SimpleErrorData struct {
		Type SimpleErrorType
		Msg  string
	}
	Integer struct {
		Data int
	}
)

func (d SimpleStringData) String() string {
	return fmt.Sprintf("%s%s%s", string(TypeSimpleString), d.Data, Terminator)
}

func (d BulkStringData) String() string {
	return fmt.Sprintf("%s%d%s%s%s", string(TypeBulkString), d.Length, Terminator, d.Data, Terminator)
}

func (d NullBulkStringData) String() string {
	return fmt.Sprintf("%s-1%s", string(TypeBulkString), Terminator)
}

func (d ArraysData) String() string {
	builder := new(strings.Builder)
	builder.WriteByte(byte(TypeArrays))
	fmt.Fprintf(builder, "%d%s", d.Length, string(Terminator))
	for ele := range slices.Values(d.Datas) {
		builder.WriteString(ele.String())
	}
	return builder.String()
}

func (d SimpleErrorData) String() string {
	return fmt.Sprintf("-%s %s\r\n", d.Type, d.Msg)
}

func Raw(data Data) string {
	return strconv.Quote(data.String())
}

func (d Integer) String() string {
	return fmt.Sprintf(":%d%s", d.Data, Terminator)
}
