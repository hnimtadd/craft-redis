package resp

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/utils"
)

type Parser struct{}

// ParseSequence parse from array of data, and get out list of Datas
func (p Parser) ParseNext(data []byte) (Data, int, error) {
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

func (p Parser) ParseSimpleStrings(input []byte) (Data, int, error) {
	utils.Assert(DataType(input[0]) == TypeSimpleString, "first byte must be simpleString indicator")

	dataStartIdx := 1
	dataEndIdx := strings.Index(string(input), Terminator)
	utils.Assert(dataEndIdx != -1)

	data := input[dataStartIdx:dataEndIdx]

	nextIdx := dataEndIdx + len(string(Terminator))
	return SimpleStringData{
		Data: string(data),
	}, nextIdx, nil
}

// $<length>\r\n<data>\r\n
func (p Parser) ParseBulkStrings(input []byte) (Data, int, error) {
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
		return NullBulkStringData{}, nextIdx, nil
	}

	dataStartIdx := respEndIdx + len(string(Terminator))
	dataEndIdx := dataStartIdx + respLength
	data := input[dataStartIdx:dataEndIdx]

	nextIdx := dataEndIdx + len(string(Terminator))
	return BulkStringData{
		Data: string(data),
	}, nextIdx, nil
}

func (p Parser) ParseArrays(input []byte) (Data, int, error) {
	utils.Assert(DataType(input[0]) == TypeArrays, "first byte must be arrays indicator")
	elesNumStartIndex := 1
	elesNumEndIndex := strings.Index(string(input), Terminator)
	utils.Assert(elesNumEndIndex != -1)

	elesNum, err := strconv.Atoi(string(input[elesNumStartIndex:elesNumEndIndex]))
	if err != nil {
		return nil, -1, fmt.Errorf("invalid number of elements: %v", err)
	}

	datas := make([]Data, elesNum)

	nextIdx := elesNumEndIndex + len(string(Terminator))

	var data Data
	for i := range elesNum {
		utils.Assert(nextIdx < len(input), "something wrong with next index")
		input = input[nextIdx:]
		data, nextIdx, err = p.ParseNext(input)
		if err != nil {
			return nil, -1, err
		}

		datas[i] = data
	}

	return ArraysData{
		Datas: datas,
	}, nextIdx, nil
}
