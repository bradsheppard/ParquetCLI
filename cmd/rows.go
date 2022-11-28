package cmd

import (
	"fmt"

	"github.com/spf13/cobra"
	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/common"
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

func rows(fileName string) {
    fr, err := local.NewLocalFileReader(fileName)

    if err != nil {
        fmt.Println("Can't open parquet file", err)
        return;
    }

    pr, err := reader.NewParquetReader(fr, nil, 4)

    if err != nil {
        fmt.Println("Can't create a parquet reader", err)
    }

    num := 10
    
    res, err := pr.ReadPartialByNumber(num, common.ReformPathStr("parquet_go_root.high"))

    if err != nil {
        fmt.Println("Error reading rows of parquet file", err)
    }

    fmt.Println(res)
}
