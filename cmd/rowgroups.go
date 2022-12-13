package cmd

import (
	"fmt"
	"parquetcli/reader"
	"parquetcli/table"

	"github.com/spf13/cobra"
)

var rowgroupsCommand = &cobra.Command{
	Use:   "rowgroups",
	Short: "Prints row groups in the parquet file",
	Run: func(cmd *cobra.Command, args []string) {
		readerInstance := new(reader.ReaderImpl)
		rowgroups(cmd, readerInstance, args[0])
	},
}

func init() {
	rootCmd.AddCommand(rowgroupsCommand)
}

func rowgroups(cmd *cobra.Command, parquetReader reader.ParquetFileReader, fileName string) {
	rowGroups, err := parquetReader.GetRowGroups(fileName, 3, 0)

	if err != nil {
		fmt.Println("Error parsing row groups", err)
		return
	}

	tb := ParseRowGroups(rowGroups)

	writer := cmd.OutOrStdout()
	table.Write(writer, tb)
}

func ParseRowGroups(rowGroups []*reader.RowGroup) *table.Table {
	tb := new(table.Table)
	tb.Header = []string{
		"Index",
		"Total Byte Size",
		"Num Rows",
	}

	for i, rowGroup := range rowGroups {
		row := []string{
			fmt.Sprint(i),
			fmt.Sprint(rowGroup.TotalByteSize),
			fmt.Sprint(rowGroup.NumRows),
		}

		tb.Rows = append(tb.Rows, row)
	}

	return tb
}
