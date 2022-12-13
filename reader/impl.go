package reader

import (
	"encoding/json"
	"fmt"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

type ReaderImpl struct{}

func (r *ReaderImpl) GetFooterInfo(file string) (*MetaData, error) {
	fr, err := local.NewLocalFileReader(file)

	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetColumnReader(fr, 4)

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
	fr, err := local.NewLocalFileReader(file)

	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetReader(fr, nil, 4)

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
			fmt.Println("Error Marshaling data", err)
			return nil, err
		}

		err = json.Unmarshal(b, &data)

		if err != nil {
			fmt.Println("Error unmarshaling data", err)
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
	fr, err := local.NewLocalFileReader(file)

	if err != nil {
		return nil, err
	}

	pr, err := reader.NewParquetColumnReader(fr, 4)

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

		result = append(result, mappedResult)
	}

	return result, nil
}
