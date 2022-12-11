package cmd

import (
	"fmt"
	"strconv"

	"parquetcli/table"
    "parquetcli/reader"

	"github.com/spf13/cobra"
)

var metaCommand = &cobra.Command{
    Use:    "meta",
    Short:  "Print metadata related to the parquet file",
    Run:    func(cmd *cobra.Command, args []string) {
        readerInstance := new(reader.ReaderImpl)
        meta(cmd, readerInstance, args[0])
    },
}

func init() {
    rootCmd.AddCommand(metaCommand)
}

func meta(cmd *cobra.Command, parquetReader reader.ParquetFileReader, fileName string) {
    info, err := parquetReader.GetFooterInfo(fileName)

    if err != nil {
        fmt.Println("Error reading parquet file footer", err)
        return
    }

    writer := cmd.OutOrStdout()

    ht := ParseMetadata(info)
    tb := ParseColumns(info)

    table.WriteHorizontal(writer, ht)
    fmt.Fprint(writer, "\n")
    table.Write(writer, tb)
}

func ParseMetadata(footer *reader.Footer) *table.HorizontalTable {
    ht := new(table.HorizontalTable)

    entries := []table.Entry{
        table.Entry{
            Header: "NumRows", 
            Value: strconv.Itoa(footer.NumRows),
        },
        table.Entry{
            Header: "EncryptionAlgorithm",
            Value: footer.EncryptionAlgorithm,
        },
        table.Entry{
            Header: "CreatedBy",
            Value: footer.CreatedBy,
        },
    }

    ht.Entries = entries
    return ht
}

func ParseColumns(footer *reader.Footer) *table.Table {
    tb := new(table.Table) 
    tb.Header = []string{"Name", "Type", "Type Length"}

    for _, column := range footer.Columns {
        row := []string{
            column.Name,
            column.Type,
            strconv.Itoa(column.TypeLength),
        }

        tb.Rows = append(tb.Rows, row)
    }

    return tb
}

