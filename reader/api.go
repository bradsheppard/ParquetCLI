package reader

type MetaData struct {
	Columns             []*Column
	NumRows             int
	EncryptionAlgorithm string
	CreatedBy           string
}

type Column struct {
	Name       string
	Type       string
	TypeLength int
}

type RowInfo struct {
	Headers []string
	Rows    [][]string
}

type RowGroup struct {
	NumRows       int
	TotalByteSize int
	ColumnChunks  []*ColumnChunk
}

type ColumnChunk struct {
	FilePath       string
	FileOffset     int
	ColumnMetaData *ColumnMetaData
}

type ColumnMetaData struct {
	PathInSchema    []string
	NumValues       int
	DataPageOffset  int
	IndexPageOffset int
}

type ParquetFileReader interface {
	GetFooterInfo(file string) (*MetaData, error)
	GetRows(file string, limit int, offset int) (*RowInfo, error)
	GetRowGroups(file string, limit int, offset int) ([]*RowGroup, error)
}
