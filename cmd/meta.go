package cmd

import (
	"fmt"
	"strconv"

	"parquetcli/table"

	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

var metaCommand = &cobra.Command{
    Use:    "meta",
    Short:  "Print metadata related to the parquet file",
    Run:    func(cmd *cobra.Command, args []string) {
        meta(cmd, args[0])
    },
}

func init() {
    rootCmd.AddCommand(metaCommand)
}

func meta(cmd *cobra.Command, fileName string) {
    fr, err := local.NewLocalFileReader(fileName)

    if err != nil {
        fmt.Println("Can't open parquet file", err)
        return;
    }

    pr, err := reader.NewParquetColumnReader(fr, 4)

    if err != nil {
        fmt.Println("Can't create a parquet reader", err)
    }

    err = pr.ReadFooter()

    if err != nil {
        fmt.Println("Error reading footer of parquet file", err)
    }

    writer := cmd.OutOrStdout()

    ht := getMetadata(pr)
    tb := getColumns(pr)

    table.WriteHorizontal(writer, ht)
    table.Write(writer, tb)

    pr.ReadStop()
    fr.Close()
}

func getMetadata(pr *reader.ParquetReader) *table.HorizontalTable {
    ht := new(table.HorizontalTable)

    entries := []table.Entry{
        table.Entry{
            Header: "NumRows", 
            Value: strconv.Itoa(int(pr.GetNumRows())),
        },
        table.Entry{
            Header: "EncryptionAlgorithm",
            Value: pr.Footer.GetEncryptionAlgorithm().String(),
        },
        table.Entry{
            Header: "CreatedBy",
            Value: pr.Footer.GetCreatedBy(),
        },
    }

    ht.Entries = entries
    return ht
}

func getColumns(pr *reader.ParquetReader) *table.Table {
    tb := new(table.Table) 
    tb.Header = []string{"Name", "Type", "Type Length"}

    for _, schemaElement := range pr.Footer.Schema {
        row := []string{
            schemaElement.GetName(),
            schemaElement.GetType().String(),
            strconv.Itoa(int(schemaElement.GetTypeLength())),
        }

        tb.Rows = append(tb.Rows, row)
    }

    return tb
}

