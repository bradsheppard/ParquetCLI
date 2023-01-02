package reader

import (
	"encoding/json"
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type ReaderImpl struct{}

func initializeReader(file string) (*reader.ParquetReader, error) {
	fr, err := local.NewLocalFileReader(file)

	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)

	if err != nil {
		return nil, err
	}

	return pr, nil
}

func initializeColumnReader(file string) (*reader.ParquetReader, error) {
	fr, err := local.NewLocalFileReader(file)

	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetColumnReader(fr, 4)

	if err != nil {
		return nil, err
	}

	return pr, nil
}

func (r *ReaderImpl) GetFooterInfo(file string) (*MetaData, error) {
	pr, err := initializeColumnReader(file)

	if err != nil {
		return nil, err
	}

	err = pr.ReadFooter()

	if err != nil {
		return nil, err
	}

	numRows := pr.Footer.GetNumRows()
	encryptionAlgorithm := pr.Footer.GetEncryptionAlgorithm().String()
	createdBy := pr.Footer.GetCreatedBy()

	columns := []*Column{}

	for _, schemaElement := range pr.Footer.Schema {
		column := Column{
			Name:       schemaElement.GetName(),
			Type:       schemaElement.GetType().String(),
			TypeLength: int(schemaElement.GetTypeLength()),
		}

		columns = append(columns, &column)
	}

	return &MetaData{
		NumRows:             int(numRows),
		Columns:             columns,
		EncryptionAlgorithm: encryptionAlgorithm,
		CreatedBy:           createdBy,
	}, nil
}

func (r *ReaderImpl) GetRows(file string, limit int, offset int) (*RowInfo, error) {
	pr, err := initializeReader(file)

	if err != nil {
		return nil, err
	}

	info := new(RowInfo)

	for _, schemaElement := range pr.Footer.Schema {
		info.Headers = append(info.Headers, schemaElement.GetName())
	}

	err = pr.SkipRows(int64(offset))

	if err != nil {
		return nil, err
	}

	res, err := pr.ReadByNumber(limit)

	for _, row := range res {
		var data map[string]interface{}

		b, err := json.Marshal(row)

		if err != nil {
			return nil, err
		}

		err = json.Unmarshal(b, &data)

		if err != nil {
			return nil, err
		}

		entries := []string{}
		for _, column := range info.Headers {
			entries = append(entries, fmt.Sprint(data[column]))
		}

		info.Rows = append(info.Rows, entries)
	}

	return info, nil
}

func (r *ReaderImpl) GetRowGroups(file string, limit int, offset int) ([]*RowGroup, error) {
	pr, err := initializeColumnReader(file)

	err = pr.ReadFooter()

	if err != nil {
		return nil, err
	}

	result := []*RowGroup{}
	rowGroups := pr.Footer.GetRowGroups()

	for _, rowGroup := range rowGroups {
		mappedResult := &RowGroup{
			NumRows:       int(rowGroup.NumRows),
			TotalByteSize: int(rowGroup.TotalByteSize),
			ColumnChunks:  []*ColumnChunk{},
		}

		for _, columnChunk := range rowGroup.GetColumns() {
			mappedColumnChunk := ColumnChunk{
				FileOffset: int(columnChunk.GetFileOffset()),
				FilePath:   columnChunk.GetFilePath(),
				ColumnMetaData: &ColumnMetaData{
					Type:             Type(columnChunk.MetaData.Type),
					PathInSchema:     columnChunk.GetMetaData().GetPathInSchema(),
					NumValues:        int(columnChunk.GetMetaData().GetNumValues()),
					DataPageOffset:   int(columnChunk.GetMetaData().GetDataPageOffset()),
					IndexPageOffset:  int(columnChunk.GetMetaData().GetIndexPageOffset()),
					CompressionCodec: CompressionCodec(columnChunk.MetaData.GetCodec()),
				},
			}

			mappedResult.ColumnChunks = append(mappedResult.ColumnChunks, &mappedColumnChunk)
		}

		result = append(result, mappedResult)
	}

	return result, nil
}
