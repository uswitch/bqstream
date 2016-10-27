package bigquery

import (
	"bytes"
	"fmt"
	bq "google.golang.org/api/bigquery/v2"
	"os"
)

type Inserter struct {
	destination *Destination
	identity    RowIdentity
	bigquery    *bq.Service

	rowBuffer     []*bq.TableDataInsertAllRequestRows
	insertedCount int64
	errorCh       chan error
	stopCh        chan bool
}

func NewInserter(dest *Destination, identity RowIdentity) (*Inserter, error) {
	svc, err := newService()
	if err != nil {
		return nil, err
	}

	return &Inserter{
		destination: dest,
		identity:    identity,
		bigquery:    svc,
		errorCh:     make(chan error, 1),
		stopCh:      make(chan bool),
	}, nil
}

func (i *Inserter) cleanBuffer() {
	i.rowBuffer = make([]*bq.TableDataInsertAllRequestRows, 0)
}

func insertErrorMessage(req *bq.TableDataInsertAllRequest, errors []*bq.TableDataInsertAllResponseInsertErrors) error {
	var b bytes.Buffer
	for _, e := range errors {
		for _, ie := range e.Errors {
			row := req.Rows[e.Index]
			b.WriteString(fmt.Sprintf("%d: %s: %+v\n", e.Index, ie.Message, row.Json))
		}
	}
	return fmt.Errorf("insert errors: %s", b.String())
}

func (i *Inserter) insert(req *bq.TableDataInsertAllRequest) (*bq.TableDataInsertAllResponse, error) {
	resp, err := i.bigquery.Tabledata.InsertAll(i.destination.ProjectID, i.destination.DatasetID, i.destination.TableID, req).Do()
	if err != nil {
		return nil, err
	}

	if len(resp.InsertErrors) > 0 {
		return nil, insertErrorMessage(req, resp.InsertErrors)
	}

	i.incrCounter(int64(len(req.Rows)))
	return resp, nil
}

func (i *Inserter) incrCounter(v int64) {
	i.insertedCount = i.insertedCount + v
}

func (i *Inserter) buffer(r *bq.TableDataInsertAllRequestRows) {
	i.rowBuffer = append(i.rowBuffer, r)
}

func (i *Inserter) insertAllBuffer() error {
	if len(i.rowBuffer) == 0 {
		return nil
	}

	req := &bq.TableDataInsertAllRequest{
		Rows:           i.rowBuffer,
		TemplateSuffix: i.destination.Suffix,
	}

	_, err := i.insert(req)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Flushed %d records", len(i.rowBuffer))

	return nil
}

func (i *Inserter) InsertedRows() int64 {
	return i.insertedCount
}

func (i *Inserter) Flush() error {
	err := i.insertAllBuffer()
	i.cleanBuffer()
	return err
}

func (i *Inserter) Insert(record map[string]bq.JsonValue) error {
	r, err := row(i.identity, record)
	if err != nil {
		return err
	}
	i.buffer(r)
	return nil
}
