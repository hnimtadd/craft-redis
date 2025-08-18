package resp

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

type Parser struct{}

type GeneralData struct {
	Data
}

// ParseSequence parse from array of data, and get out list of Datas
func (p Parser) ParseNext(data []byte) (*GeneralData, int, error) {
	if len(data) == 0 {
		return nil, -1, errors.New("data must not be nil")
	}

	typ := data[0]
	switch DataType(typ) {
	case TypeSimpleString:
		return p.ParseSimpleStrings(data)
	case TypeBulkString:
		return p.ParseBulkStrings(data)
	case TypeArrays:
		return p.ParseArrays(data)
	default:
		return nil, -1, fmt.Errorf("unsupport data type: %v", string(typ))
	}
}

type Data interface {
	String() string
	RawString() string
}
type SimpleStringData struct {
	Data string
}

func (d SimpleStringData) String() string {
	return fmt.Sprintf("%s%s%s", string(TypeSimpleString), d.Data, Terminator)
}

func (d SimpleStringData) RawString() string {
	return strconv.Quote(d.String())
}

func (p Parser) ParseSimpleStrings(input []byte) (*GeneralData, int, error) {
	utils.Assert(DataType(input[0]) == TypeSimpleString, "first byte must be simpleString indicator")

	dataStartIdx := 1
	dataEndIdx := strings.Index(string(input), Terminator)
	utils.Assert(dataEndIdx != -1)

	data := input[dataStartIdx:dataEndIdx]

	nextIdx := dataEndIdx + len(string(Terminator))
	return &GeneralData{
		Data: SimpleStringData{
			Data: string(data),
		},
	}, nextIdx, nil
}

type NullBulkStringData struct{}

func (d NullBulkStringData) String() string {
	return fmt.Sprintf("%s-1%s", string(TypeBulkString), Terminator)
}

func (d NullBulkStringData) RawString() string {
	return strconv.Quote(d.String())
}

type BulkStringData struct {
	Length int
	Data   string
}

func (d BulkStringData) String() string {
	return fmt.Sprintf("%s%d%s%s%s", string(TypeBulkString), d.Length, Terminator, d.Data, Terminator)
}

func (d BulkStringData) RawString() string {
	return strconv.Quote(d.String())
}

// $<length>\r\n<data>\r\n
func (p Parser) ParseBulkStrings(input []byte) (*GeneralData, int, error) {
	utils.Assert(DataType(input[0]) == TypeBulkString, "first byte must be bulkStrings indicator")
	respStartIdx := 1
	respEndIdx := strings.Index(string(input), string(Terminator))
	utils.Assert(respEndIdx != -1)

	respLength, err := strconv.Atoi(string(input[respStartIdx:respEndIdx]))
	if err != nil {
		return nil, -1, fmt.Errorf("failed to parse resp length: %v", err)
	}
	if respLength == -1 {
		nextIdx := respEndIdx + len(string(Terminator))
		// NULL bulkString
		return &GeneralData{
			Data: NullBulkStringData{},
		}, nextIdx, nil
	}

	dataStartIdx := respEndIdx + len(string(Terminator))
	dataEndIdx := dataStartIdx + respLength
	data := input[dataStartIdx:dataEndIdx]

	nextIdx := dataEndIdx + len(string(Terminator))
	return &GeneralData{
		Data: BulkStringData{
			Length: respLength,
			Data:   string(data),
		},
	}, nextIdx, nil
}

type ArraysData struct {
	Length int
	Datas  []GeneralData
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

func (d ArraysData) RawString() string {
	return strconv.Quote(d.String())
}

func (p Parser) ParseArrays(input []byte) (*GeneralData, int, error) {
	utils.Assert(DataType(input[0]) == TypeArrays, "first byte must be arrays indicator")
	elesNumStartIndex := 1
	elesNumEndIndex := strings.Index(string(input), Terminator)
	utils.Assert(elesNumEndIndex != -1)

	elesNum, err := strconv.Atoi(string(input[elesNumStartIndex:elesNumEndIndex]))
	if err != nil {
		return nil, -1, fmt.Errorf("invalid number of elements: %v", err)
	}

	datas := make([]GeneralData, elesNum)

	nextIdx := elesNumEndIndex + len(string(Terminator))

	var data *GeneralData
	for i := range elesNum {
		utils.Assert(nextIdx < len(input), "something wrong with next index")
		input = input[nextIdx:]
		data, nextIdx, err = p.ParseNext(input)
		if err != nil {
			return nil, -1, err
		}

		datas[i] = *data
	}

	return &GeneralData{
		Data: ArraysData{
			Length: elesNum,
			Datas:  datas,
		},
	}, nextIdx, nil
}
