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
			ColumnChunks: []*reader.ColumnChunk{
				&reader.ColumnChunk{
					FilePath:   "Path 1",
					FileOffset: 1,
					ColumnMetaData: &reader.ColumnMetaData{
						PathInSchema: []string{"Path1Part1", "Path1Part2"},
					},
				},
			},
		},
		&reader.RowGroup{
			NumRows:       20,
			TotalByteSize: 200,
			ColumnChunks: []*reader.ColumnChunk{
				&reader.ColumnChunk{
					FilePath:   "Path 2",
					FileOffset: 2,
					ColumnMetaData: &reader.ColumnMetaData{
						PathInSchema: []string{"Path2Part1", "Path2Part2"},
					},
				},
			},
		},
	}

	infos := ParseRowGroups(rowGroups)

	expectedRowGroup1 := new(RowGroupInfo)
	expectedRowGroup2 := new(RowGroupInfo)

	expected := []*RowGroupInfo{expectedRowGroup1, expectedRowGroup2}

	expectedRowGroup1.Header = &table.HorizontalTable{
		Entries: []table.Entry{
			table.Entry{
				Header: "Number",
				Value:  "0",
			},
			table.Entry{
				Header: "Total Byte Size",
				Value:  "100",
			},
			table.Entry{
				Header: "Num Rows",
				Value:  "10",
			},
		},
	}
	expectedRowGroup1.ColumnChunks = &table.Table{}
	expectedRowGroup1.ColumnChunks.Header = []string{
		"Index",
		"File Path",
		"File Offset",
		"Path In Schema",
	}
	expectedRowGroup1.ColumnChunks.Rows = [][]string{
		[]string{
			"0",
			"Path 1",
			"1",
			"Path1Part1/Path1Part2",
		},
	}

	expectedRowGroup2.Header = &table.HorizontalTable{
		Entries: []table.Entry{
			table.Entry{
				Header: "Number",
				Value:  "1",
			},
			table.Entry{
				Header: "Total Byte Size",
				Value:  "200",
			},
			table.Entry{
				Header: "Num Rows",
				Value:  "20",
			},
		},
	}
	expectedRowGroup2.ColumnChunks = &table.Table{}
	expectedRowGroup2.ColumnChunks.Header = []string{
		"Index",
		"File Path",
		"File Offset",
		"Path In Schema",
	}
	expectedRowGroup2.ColumnChunks.Rows = [][]string{
		[]string{
			"0",
			"Path 2",
			"2",
			"Path2Part1/Path2Part2",
		},
	}

	assert.Equal(t, expected, infos)
}
