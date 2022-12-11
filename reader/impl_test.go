package reader

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

var file = "../prices.parquet"

func TestGetFooterInfo(t *testing.T) {
    reader := ReaderImpl{}
    info, err := reader.GetFooterInfo(file)

    if err != nil {
        t.Error(err)
    }

    expected_info := &Footer{
        Columns: []Column{
            Column{
                Name: "schema",
                Type: "BOOLEAN",
                TypeLength: 0,
            },
            Column{
                Name: "ticker",
                Type: "BYTE_ARRAY",
                TypeLength: 0,
            },
            Column{
                Name: "date",
                Type: "BYTE_ARRAY",
                TypeLength: 0,
            },
            Column{
                Name: "open",
                Type: "DOUBLE",
                TypeLength: 64,
            },
            Column{
                Name: "high",
                Type: "DOUBLE",
                TypeLength: 64,
            },
            Column{
                Name: "low",
                Type: "DOUBLE",
                TypeLength: 64,
            },
            Column{
                Name: "close",
                Type: "DOUBLE",
                TypeLength: 64,
            },
        },
        NumRows: 5792,
        CreatedBy: "fastparquet-python version 0.8.1 (build 0)",
        EncryptionAlgorithm: "<nil>",
    }

    assert.Equal(t, expected_info, info)
}

func TestGetRows(t *testing.T) {
    reader := ReaderImpl{}
    rows, err := reader.GetRows(file, 2, 0)

    if err != nil {
        t.Error(err)
    }

    expected_rows := &RowInfo{
        Headers: []string{
            "Schema",
            "Ticker",
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
        },
        Rows: [][]string {
            []string {
                "<nil>",
                "IBM",
                "2022-11-04",
                "135.65",
                "137.73",
                "134.94",
                "136.96",
            },
            []string {
                "<nil>",
                "IBM",
                "2022-11-03",
                "136.42",
                "136.48",
                "133.97",
                "134.47",
            },
        },
    }

    assert.Equal(t, expected_rows, rows)
}

func TestGetRowsWithOffset(t *testing.T) {
    reader := ReaderImpl{}
    rows, err := reader.GetRows(file, 2, 1)

    if err != nil {
        t.Error(err)
    }

    expected_rows := &RowInfo{
        Headers: []string{
            "Schema",
            "Ticker",
            "Date",
            "Open",
            "High",
            "Low",
            "Close",
        },
        Rows: [][]string {
            []string {
                "<nil>",
                "IBM",
                "2022-11-03",
                "136.42",
                "136.48",
                "133.97",
                "134.47",
            },
            []string {
                "<nil>",
                "IBM",
                "2022-11-02",
                "137.75",
                "140.17",
                "136.8",
                "136.83",
            },
        },
    }

    assert.Equal(t, expected_rows, rows)
}

