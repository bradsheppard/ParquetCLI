package cmd

import (
    "parquetcli/reader"
	"parquetcli/table"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestMeta_GetMetadata(t *testing.T) {
    footerInfo := reader.Footer{
        NumRows: 5792,
        EncryptionAlgorithm: "Test Encryption Algorithm",
        CreatedBy: "Test Program",
    }

    ht := ParseMetadata(&footerInfo)

    expected := new(table.HorizontalTable)
    expected.Entries = []table.Entry{
        table.Entry{
            Header: "NumRows",
            Value: "5792",
        },
        table.Entry{
            Header: "EncryptionAlgorithm",
            Value: "Test Encryption Algorithm",
        },
        table.Entry{
            Header: "CreatedBy",
            Value: "Test Program",
        },
    }

    assert.Equal(t, expected, ht)
}

func TestMeta_GetColumns(t *testing.T) {
    footerInfo := reader.Footer{
        Columns: []reader.Column{
            reader.Column{
               Name: "Test Column 1", 
               Type: "Test Type 1",
               TypeLength: 11,
            },
            reader.Column{
                Name: "Test Column 2",
                Type: "Test Type 2",
                TypeLength: 22,
            },
        },
    }

    tb := ParseColumns(&footerInfo)

    expected := new(table.Table)
    expected.Header = []string{
        "Name",
        "Type",
        "Type Length",
    }

    expected.Rows = [][]string{
        []string{
            "Test Column 1",
            "Test Type 1",
            "11",
        },
        []string{
            "Test Column 2",
            "Test Type 2",
            "22",
        },
    }

    assert.Equal(t, expected, tb)
}

