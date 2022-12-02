package cmd

import (
	"encoding/json"
	"fmt"
	"parquetcli/table"

	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

var limit int
var offset int64

var rowsCommand = &cobra.Command{
    Use:    "rows",
    Short:  "Prints the rows contained in the parquet file",
    Run:    func(cmd *cobra.Command, args []string) {
        rows(cmd, args[0], limit, offset)
    },
}

func init() {
    rowsCommand.Flags().IntVarP(&limit, "limit", "l", 10, "The maximum number of rows to return")
    rowsCommand.Flags().Int64VarP(&offset, "offset", "o", 0, "The offset from the first row")

    rootCmd.AddCommand(rowsCommand)
}

func getRowTable(pr *reader.ParquetReader, limit int, offset int64) (*table.Table, error) {
    columns := []string{}

    for _, schemaElement := range pr.Footer.Schema {
        columns = append(columns, schemaElement.GetName())
    }

    err := pr.SkipRows(offset)
    res, err := pr.ReadByNumber(limit)

    if err != nil {
        fmt.Println("Error reading rows of parquet file", err)
    }

    tb := new(table.Table)
    tb.Header = columns

    for _, row := range res {
        var data map[string]interface{}

        b, err := json.Marshal(row)

        if err != nil {
            fmt.Println("Error Marshaling data", err)
            return nil, err
        }

        err = json.Unmarshal(b, &data)

        if err != nil {
            fmt.Println("Error unmarshaling data", err)
            return nil, err
        }

        entries := []string{}
        for _, column := range columns {
            entries = append(entries, fmt.Sprint(data[column]))
        }

        tb.Rows = append(tb.Rows, entries)
    }

    return tb, nil
}

func rows(cmd *cobra.Command, fileName string, limit int, offset int64) {
    fr, err := local.NewLocalFileReader(fileName)

    if err != nil {
        fmt.Println("Can't open parquet file", err)
        return
    }

    pr, err := reader.NewParquetReader(fr, nil, 4)

    if err != nil {
        fmt.Println("Can't create a parquet reader", err)
        return
    }

    tb, err := getRowTable(pr, limit, offset)

    if err != nil {
        fmt.Println("Error generating row table", err)
        return
    }

    writer := cmd.OutOrStdout()
    table.Write(writer, tb)

    pr.ReadStop()
    fr.Close()
}
