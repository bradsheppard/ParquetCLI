package cmd

import (
	"fmt"
	"parquetcli/reader"
	"parquetcli/table"
	"strings"

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

type RowGroupInfo struct {
	Header       *table.HorizontalTable
	ColumnChunks *table.Table
}

func rowgroups(cmd *cobra.Command, parquetReader reader.ParquetFileReader, fileName string) {
	rowGroups, err := parquetReader.GetRowGroups(fileName, 3, 0)

	if err != nil {
		fmt.Println("Error parsing row groups", err)
		return
	}

	infos := ParseRowGroups(rowGroups)
	writer := cmd.OutOrStdout()

	for _, info := range infos {
		table.WriteHorizontal(writer, info.Header)
		fmt.Println()
		table.WriteWithSpacing(writer, info.ColumnChunks, 16, 4, 4)
		fmt.Println()
	}
}

func ParseRowGroups(rowGroups []*reader.RowGroup) []*RowGroupInfo {
	infos := []*RowGroupInfo{}

	for i, rowGroup := range rowGroups {
		tb := new(table.HorizontalTable)
		info := new(RowGroupInfo)

		tb.Entries = []table.Entry{
			table.Entry{
				Header: "Row Group Index",
				Value:  fmt.Sprint(i),
			},
			table.Entry{
				Header: "Total Byte Size",
				Value:  fmt.Sprint(rowGroup.TotalByteSize),
			},
			table.Entry{
				Header: "Num Rows",
				Value:  fmt.Sprint(rowGroup.NumRows),
			},
		}

		colTb := new(table.Table)
		colTb.Header = []string{
			"Type",
			"File Path",
			"File Offset",
			"Path In Schema",
			"Num Values",
			"Compression Codec",
			"Data Page Offset",
			"Index Page Offset",
		}

		for _, columnChunk := range rowGroup.ColumnChunks {
			row := []string{
				columnChunk.ColumnMetaData.Type.String(),
				columnChunk.FilePath,
				fmt.Sprint(columnChunk.FileOffset),
				strings.Join(columnChunk.ColumnMetaData.PathInSchema, "/"),
				fmt.Sprint(columnChunk.ColumnMetaData.NumValues),
				columnChunk.ColumnMetaData.CompressionCodec.String(),
				fmt.Sprint(columnChunk.ColumnMetaData.DataPageOffset),
				fmt.Sprint(columnChunk.ColumnMetaData.IndexPageOffset),
			}

			colTb.Rows = append(colTb.Rows, row)
		}

		info.Header = tb
		info.ColumnChunks = colTb

		infos = append(infos, info)
	}

	return infos
}
