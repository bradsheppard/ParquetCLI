package reader

type Footer struct {
	Columns             []Column
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

type ParquetFileReader interface {
	GetFooterInfo(file string) (*Footer, error)
	GetRows(file string, limit int, offset int) (*RowInfo, error)
}
