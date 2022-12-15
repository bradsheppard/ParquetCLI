package cmd

import (
	"parquetcli/reader"
	"parquetcli/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRowGroups(t *testing.T) {
	rowGroups := []*reader.RowGroup{
		&reader.RowGroup{
			NumRows:       10,
			TotalByteSize: 100,
			ColumnChunks:  []*reader.ColumnChunk{},
		},
		&reader.RowGroup{
			NumRows:       20,
			TotalByteSize: 200,
			ColumnChunks:  []*reader.ColumnChunk{},
		},
	}

	tb := ParseRowGroups(rowGroups)

	expected := new(table.Table)
	expected.Header = []string{
		"Index",
		"Total Byte Size",
		"Num Rows",
	}

	expected.Rows = [][]string{
		[]string{
			"0",
			"100",
			"10",
		},
		[]string{
			"1",
			"200",
			"20",
		},
	}

	assert.Equal(t, expected, tb)
}
