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

func ScanRecords(reader *bufio.Reader, ch chan<- map[string]bq.JsonValue) error {
	for {
		buf, err := reader.ReadBytes('\n')
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return fmt.Errorf("error reading next: %s", err.Error())
		}

		var out map[string]bq.JsonValue
		err = json.Unmarshal(buf, &out)
		if err != nil {
			return fmt.Errorf("error unmarshaling json: %s", err.Error())
		}

		ch <- out
	}
	return nil
}
