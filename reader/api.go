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
	Type             Type
	PathInSchema     []string
	NumValues        int
	DataPageOffset   int
	IndexPageOffset  int
	KeyValueMetadata []string
}

type Type int64

const (
	BOOLEAN                 Type = 0
	INT32                   Type = 1
	INT64                   Type = 2
	INT96                   Type = 3
	FLOAT                   Type = 4
	DOUBLE                  Type = 5
	BYTE_ARRAY              Type = 6
	FIXED_LENGTH_BYTE_ARRAY Type = 7
)

func (p Type) String() string {
	switch p {
	case BOOLEAN:
		return "BOOLEAN"
	case INT32:
		return "INT32"
	case INT64:
		return "INT64"
	case INT96:
		return "INT96"
	case FLOAT:
		return "FLOAT"
	case DOUBLE:
		return "DOUBLE"
	case BYTE_ARRAY:
		return "BYTE_ARRAY"
	case FIXED_LENGTH_BYTE_ARRAY:
		return "FIXED_LEN_BYTE_ARRAY"
	}
	return "<UNSET>"
}

type ParquetFileReader interface {
	GetFooterInfo(file string) (*MetaData, error)
	GetRows(file string, limit int, offset int) (*RowInfo, error)
	GetRowGroups(file string, limit int, offset int) ([]*RowGroup, error)
}
