package main

import (
	"bufio"
	"fmt"
	"github.com/uswitch/bqstream/bigquery"
	bq "google.golang.org/api/bigquery/v2"
	"gopkg.in/alecthomas/kingpin.v2"
	"os"
	"os/signal"
	"time"
)

var (
	app           = kingpin.New("bqstream", "Stream newline-delimited JSON to BigQuery")
	projectId     = kingpin.Flag("project-id", "Google Cloud Project ID").Required().String()
	datasetId     = kingpin.Flag("dataset-id", "BigQuery Dataset ID").Required().String()
	tableId       = kingpin.Flag("table-id", "BigQuery Table ID. If a suffix is used, data will be inserted into table-id_suffix.").Required().String()
	suffix        = kingpin.Flag("table-suffix", "BigQuery Table suffix. Can be used when time sharding tables. YYYYMMDD").String()
	insertId      = kingpin.Flag("insert-id", "Attribute name in JSON record that uniquely identifies record. Can be used to deduplicate BigQuery insertions.").String()
	flushInterval = kingpin.Flag("flush-interval", "How frequently to stream records to BigQuery.").Default("5s").Duration()
)

func identity() bigquery.RowIdentity {
	if *insertId == "" {
		return bigquery.NewEmptyIdentity()
	} else {
		return bigquery.NewAttributeIdentity(*insertId)
	}
}

func main() {
	kingpin.Parse()

	client := bigquery.New()
	reader := bufio.NewReader(os.Stdin)
	destination := &bigquery.Destination{
		ProjectID: *projectId,
		DatasetID: *datasetId,
		TableID:   *tableId,
	}
	if *suffix != "" {
		destination.Suffix = *suffix
	}

	exists, err := client.DestinationExists(destination)
	if err != nil {
		fmt.Println("ERROR: error checking if destination exists:", err.Error())
		os.Exit(1)
	}

	if !exists {
		fmt.Println("ERROR: destination doesn't exist, please create first.")
		os.Exit(1)
	}

	inserter, err := bigquery.NewInserter(destination, identity())
	go flushOnInterrupt(inserter)
	go flusher(inserter)
	if err != nil {
		fmt.Println("ERROR:", err.Error())
	}
	ch := make(chan map[string]bq.JsonValue)
	go func() {
		for m := range ch {
			err := inserter.Insert(m)
			if err != nil {
				fmt.Println("ERROR:", err.Error())
				os.Exit(1)
			}
		}
	}()

	err = bigquery.ScanRecords(reader, ch)
	if err != nil {
		fmt.Println("ERROR:", err.Error())
		os.Exit(1)
	}

	fmt.Printf("Inserted %d rows\n", inserter.InsertedRows())
}

func flusher(inserter *bigquery.Inserter) {
	for _ = range time.Tick(*flushInterval) {
		err := inserter.Flush()
		if err != nil {
			fmt.Println("ERROR:", err.Error())
			os.Exit(1)
		}
	}
}

func flushOnInterrupt(inserter *bigquery.Inserter) {
	done := make(chan bool)
	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	go func() {
		for _ = range signalChan {
			inserter.Flush()
			done <- true
		}
	}()
	<-done
}
