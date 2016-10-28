package bigquery

import (
	"bytes"
	"fmt"
	bq "google.golang.org/api/bigquery/v2"
	"os"
	"sync"
)

type Inserter struct {
	sync.RWMutex

	destination *Destination
	identity    RowIdentity
	bigquery    *bq.Service

	rowBuffer      []*bq.TableDataInsertAllRequestRows
	flushSize      int
	insertedCount  int64
	ignoreUnknowns bool
}

type InserterOpts struct {
	Destination    *Destination
	Identity       RowIdentity
	IgnoreUnknowns bool
	FlushSize      int
}

func NewInserter(opts *InserterOpts) (*Inserter, error) {
	svc, err := newService()
	if err != nil {
		return nil, err
	}

	return &Inserter{
		destination:    opts.Destination,
		identity:       opts.Identity,
		bigquery:       svc,
		ignoreUnknowns: opts.IgnoreUnknowns,
		flushSize:      opts.FlushSize,
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

func (i *Inserter) currentInsertAllRequest() *bq.TableDataInsertAllRequest {
	req := &bq.TableDataInsertAllRequest{
		Rows:                i.rowBuffer,
		IgnoreUnknownValues: i.ignoreUnknowns,
	}

	if i.destination.Suffix != "" {
		req.TemplateSuffix = fmt.Sprintf("_%s", i.destination.Suffix)
	}

	return req
}

func (i *Inserter) insertAllBuffer() error {
	if len(i.rowBuffer) == 0 {
		return nil
	}

	_, err := i.insert(i.currentInsertAllRequest())
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "Flushed %d records\n", len(i.rowBuffer))

	return nil
}

func (i *Inserter) InsertedRows() int64 {
	return i.insertedCount
}

func (i *Inserter) Flush() error {
	i.Lock()
	defer i.Unlock()

	err := i.insertAllBuffer()
	i.cleanBuffer()

	return err
}

func (i *Inserter) needToFlush() bool {
	return len(i.rowBuffer) == i.flushSize
}

func (i *Inserter) Insert(record map[string]bq.JsonValue) error {
	r, err := row(i.identity, record)
	if err != nil {
		return err
	}
	i.buffer(r)

	if i.needToFlush() {
		i.Flush()
	}

	return nil
}
