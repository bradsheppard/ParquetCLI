package cmd

import (
	"encoding/json"
	"fmt"
	"parquetcli/table"

	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

var rowsCommand = &cobra.Command{
    Use:    "rows",
    Short:  "Prints the rows contained in the parquet file",
    Run:    func(cmd *cobra.Command, args []string) {
        rows(args[0])
    },
}

func init() {
    rootCmd.AddCommand(rowsCommand)
}

func getRowTable(pr *reader.ParquetReader) *table.Table {
    num := 10

    res, err := pr.ReadByNumber(num)

    if err != nil {
        fmt.Println("Error reading rows of parquet file", err)
    }

    tb := new(table.Table)

    for _, row := range res {
        var data map[string]interface{}

        b, err := json.Marshal(row)

        if err != nil {
            fmt.Println("Error Marshaling data", err)
        }

        err = json.Unmarshal(b, &data)

        if err != nil {
            fmt.Println("Error unmarshaling data", err)
        }

        entries := []string{}

        for key := range data {
            entries = append(entries, key)
        }

        tb.Rows = append(tb.Rows, entries)
    }

    return tb
}

func rows(fileName string) {
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

    num := 10
    
    res, err := pr.ReadByNumber(num)

    if err != nil {
        fmt.Println("Error reading rows of parquet file", err)
    }

    for _, x := range res {
        var data map[string]interface{}

        b, err := json.Marshal(x)
        err = json.Unmarshal(b, &data)

        if err != nil {
            return
        }
        fmt.Println(data)
    }

}
