package bigquery

import (
	"bufio"
	"encoding/json"
	"fmt"
	"golang.org/x/net/context"
	"golang.org/x/oauth2/google"
	bq "google.golang.org/api/bigquery/v2"
	"io"
)

func NewChannel() chan map[string]bq.JsonValue {
	return make(chan map[string]bq.JsonValue)
}

type Client struct {
}

type Destination struct {
	ProjectID string
	DatasetID string
	TableID   string
	Suffix    string
}

func New() *Client {
	return &Client{}
}

func (c *Client) DestinationExists(dest *Destination) (bool, error) {
	svc, err := newService()
	if err != nil {
		return false, err
	}

	table, err := svc.Tables.Get(dest.ProjectID, dest.DatasetID, dest.TableID).Do()
	if err != nil {
		return false, err
	}

	return table != nil, nil
}

func newService() (*bq.Service, error) {
	ctx := context.Background()
	client, err := google.DefaultClient(ctx, bq.BigqueryScope)
	if err != nil {
		return nil, err
	}
	svc, err := bq.New(client)
	if err != nil {
		return nil, err
	}
	return svc, nil
}

func row(id RowIdentity, m map[string]bq.JsonValue) (*bq.TableDataInsertAllRequestRows, error) {
	identity, err := id.Identity(m)
	if err != nil {
		return nil, err
	}
	row := &bq.TableDataInsertAllRequestRows{
		InsertId: identity,
		Json:     m,
	}
	return row, nil
}

type RowIdentity interface {
	Identity(map[string]bq.JsonValue) (string, error)
}

type AttributeIdentity struct{ name string }

func NewAttributeIdentity(name string) *AttributeIdentity {
	return &AttributeIdentity{name}
}

func (a *AttributeIdentity) Identity(m map[string]bq.JsonValue) (string, error) {
	val, ok := m[a.name]
	if !ok {
		return "", fmt.Errorf("no value for insertId attribute %s in record.", a.name)
	}
	return val.(string), nil
}

type EmptyIdentity struct{}

func (e *EmptyIdentity) Identity(m map[string]bq.JsonValue) (string, error) {
	return "", nil
}

func NewEmptyIdentity() *EmptyIdentity {
	return &EmptyIdentity{}
}

func (c *Client) Stream(reader io.Reader, ch chan<- map[string]bq.JsonValue) error {
	scanner := bufio.NewScanner(reader)
	for scanner.Scan() {
		var out map[string]bq.JsonValue
		err := json.Unmarshal(scanner.Bytes(), &out)
		if err != nil {
			return err
		}

		ch <- out
	}
	return nil
}
