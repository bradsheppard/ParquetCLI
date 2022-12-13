package cmd

import (
	"fmt"

	"parquetcli/reader"
	"parquetcli/table"

	"github.com/spf13/cobra"
)

var limit int
var offset int

var rowsCommand = &cobra.Command{
	Use:   "rows",
	Short: "Prints the rows contained in the parquet file",
	Run: func(cmd *cobra.Command, args []string) {
		readerInstance := new(reader.ReaderImpl)
		rows(cmd, readerInstance, args[0], limit, offset)
	},
}

func init() {
	rowsCommand.Flags().IntVarP(&limit, "limit", "l", 10, "The maximum number of rows to return")
	rowsCommand.Flags().IntVarP(&offset, "offset", "o", 0, "The offset from the first row")

	rootCmd.AddCommand(rowsCommand)
}

func rows(cmd *cobra.Command, parquetReader reader.ParquetFileReader, fileName string, limit int, offset int) {
	rows, err := parquetReader.GetRows(fileName, limit, offset)

	if err != nil {
		fmt.Println("Error parsing rows", err)
		return
	}

	tb := ParseRows(rows)

	writer := cmd.OutOrStdout()
	table.Write(writer, tb)
}

func ParseRows(rows *reader.RowInfo) *table.Table {
	tb := new(table.Table)
	tb.Header = rows.Headers
	tb.Rows = rows.Rows

	return tb
}
