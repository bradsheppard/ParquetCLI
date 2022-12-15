package cmd

import (
	"parquetcli/reader"
	"parquetcli/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestParseRows(t *testing.T) {
	rows := reader.RowInfo{
		Headers: []string{
			"Header 1",
			"Header 2",
		},
		Rows: [][]string{
			[]string{
				"Entry 11",
				"Entry 12",
			},
			[]string{
				"Entry 21",
				"Entry 22",
			},
		},
	}

	tb := ParseRows(&rows)

	expected := new(table.Table)
	expected.Header = []string{
		"Header 1", "Header 2",
	}
	expected.Rows = [][]string{
		[]string{"Entry 11", "Entry 12"},
		[]string{"Entry 21", "Entry 22"},
	}

	assert.Equal(t, expected, tb)
}
