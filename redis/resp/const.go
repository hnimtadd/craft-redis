package resp

const (
	Terminator = "\r\n"
)

// RESP3 compatible datatype
type DataType byte

const (
	TypeSimpleString    DataType = '+'
	TypeSimpleError     DataType = '-'
	TypeIntegers        DataType = ':'
	TypeBulkString      DataType = '$'
	TypeNullBulkString  DataType = '$'
	TypeArrays          DataType = '*'
	TypeNulls           DataType = '_'
	TypeBooleans        DataType = '#'
	TypeDoubles         DataType = ','
	TypeBigNumbers      DataType = '('
	TypeBulkErrors      DataType = '!'
	TypeVerbatimStrings DataType = '='
	TypeMaps            DataType = '%'
	TypeAttributes      DataType = '|'
	TypeSets            DataType = '~'
	TypePushes          DataType = '>'
)

type SimpleErrorType string

const (
	SimpleErrorTypeGeneric   SimpleErrorType = "ERR"
	SimpleErrorTypeWrongType SimpleErrorType = "WRONGTYPE"
)
