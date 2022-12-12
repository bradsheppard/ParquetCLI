package table

import (
	"fmt"
	"io"
	"text/tabwriter"
)

type Table struct {
	Header []string
	Rows   [][]string
}

type HorizontalTable struct {
	Entries []Entry
}

type Entry struct {
	Header string
	Value  string
}

func Write(writer io.Writer, table *Table) {
	w := tabwriter.NewWriter(writer, 25, 8, 1, ' ', 0)

	for _, column := range table.Header {
		fmt.Fprint(w, column)
		fmt.Fprint(w, "\t")
	}

	fmt.Fprint(w, "\n")

	for _, row := range table.Rows {
		for _, cell := range row {
			fmt.Fprint(w, cell)
			fmt.Fprint(w, "\t")
		}

		fmt.Fprint(w, "\n")
	}

	w.Flush()
}

func WriteHorizontal(writer io.Writer, horizontalTable *HorizontalTable) {
	w := tabwriter.NewWriter(writer, 25, 8, 1, ' ', 0)

	for _, entry := range horizontalTable.Entries {
		fmt.Fprint(w, entry.Header)
		fmt.Fprint(w, "\t")
		fmt.Fprint(w, entry.Value)
		fmt.Fprint(w, "\n")
	}

	w.Flush()
}
