package cmd

import (
	"fmt"
	"parquetcli/table"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

func TestMeta_GetMetadata(t *testing.T) {
    fr, err := local.NewLocalFileReader("../prices.parquet")

    if err != nil {
        fmt.Println("Can't open parquet file", err)
        return;
    }

    pr, err := reader.NewParquetColumnReader(fr, 4)

    ht := getMetadata(pr)

    expected := new(table.HorizontalTable)
    expected.Entries = []table.Entry{
        table.Entry{
            Header: "NumRows",
            Value: "5792",
        },
        table.Entry{
            Header: "EncryptionAlgorithm",
            Value: "<nil>",
        },
        table.Entry{
            Header: "CreatedBy",
            Value: "fastparquet-python version 0.8.1 (build 0)",
        },
    }

    assert.Equal(t, expected, ht)
}

func TestMeta_GetColumns(t *testing.T) {
    fr, err := local.NewLocalFileReader("../prices.parquet")

    if err != nil {
        fmt.Println("Can't open parquet file", err)
        return;
    }
    
    pr, err := reader.NewParquetColumnReader(fr, 4)

    tb := getColumns(pr)

    expected := new(table.Table)
    expected.Header = []string{
        "Name",
        "Type",
        "Type Length",
    }

    expected.Rows = [][]string{
        []string{
            "Schema",
            "BOOLEAN",
            "0",
        },
        []string{
            "Ticker",
            "BYTE_ARRAY",
            "0",
        },
        []string{
            "Date",
            "BYTE_ARRAY",
            "0",
        },
        []string{
            "Open",
            "DOUBLE",
            "64",
        },
        []string{
            "High",
            "DOUBLE",
            "64",
        },
        []string{
            "Low",
            "DOUBLE",
            "64",
        },
        []string{
            "Close",
            "DOUBLE",
            "64",
        },
    }

    assert.Equal(t, expected, tb)
}

