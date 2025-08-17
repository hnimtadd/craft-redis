package resp

const (
	Terminator = "\r\n"
)

type DataType byte

const (
	TypeSimpleString    DataType = '+'
	TypeSimpleError     DataType = '-'
	TypeIntegers        DataType = ':'
	TypeBulkString      DataType = '$'
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
