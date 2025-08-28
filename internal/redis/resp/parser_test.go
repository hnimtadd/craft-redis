package resp

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParserSimpleString(t *testing.T) {
	tcs := []struct {
		name          string
		input         []byte
		expectedData  SimpleStringData
		expectedError bool
	}{
		{
			name:          "happy case",
			input:         []byte(`+OK\r\n`),
			expectedError: false,
			expectedData: SimpleStringData{
				Data: "OK",
			},
		},
	}
	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			parser := Parser{}
			data, _, err := parser.ParseSimpleStrings(tc.input)
			assert.Equal(t, tc.expectedError, err != nil)
			assert.EqualValues(t, data.Data, tc.expectedData)
			assert.Equal(t, string(tc.input), data.String())
		})
	}
}

func TestParserArrays(t *testing.T) {
	tcs := []struct {
		name          string
		input         []byte
		expectedData  *ArraysData
		expectedError bool
	}{
		{
			name:          "happy case",
			input:         []byte(`*0\r\n`),
			expectedError: false,
			expectedData: &ArraysData{
				Datas: []GeneralData{},
			},
		},
		{
			name:          "array of bulkStrings",
			input:         []byte(`*2\r\n$5\r\nhello\r\n$5\r\nworld\r\n`),
			expectedError: false,
			expectedData: &ArraysData{
				Length: 2,
				Datas: []GeneralData{
					{
						Data: BulkStringData{Length: 5, Data: "hello"},
					},
					{
						Data: BulkStringData{Length: 5, Data: "world"},
					},
				},
			},
		},
		{
			name:          "ECHO banana",
			input:         []byte(`*2\r\n$4\r\nECHO\r\n$6\r\nbanana\r\n`),
			expectedError: false,
			expectedData: &ArraysData{
				Length: 2,
				Datas: []GeneralData{
					{
						Data: BulkStringData{Length: 4, Data: "ECHO"},
					},
					{
						Data: BulkStringData{Length: 6, Data: "banana"},
					},
				},
			},
		},

		{
			name:          "ECHO hey",
			input:         []byte(`*2\r\n$4\r\nECHO\r\n$3\r\nhey\r\n`),
			expectedError: false,
			expectedData: &ArraysData{
				Length: 2,
				Datas: []GeneralData{
					{
						Data: BulkStringData{Length: 4, Data: "ECHO"},
					},
					{
						Data: BulkStringData{Length: 3, Data: "hey"},
					},
				},
			},
		},
	}

	for _, tc := range tcs {
		t.Run(tc.name, func(t *testing.T) {
			parser := Parser{}
			data, _, err := parser.ParseArrays(tc.input)
			if tc.expectedError {
				assert.Error(t, err)
			} else {
				assert.Nil(t, err)
			}
			if tc.expectedData != nil {
				assert.NotNil(t, data)
				assert.EqualValues(t, data.Data, *tc.expectedData)
				assert.Equal(t, string(tc.input), data.String())
			}
		})
	}
}
