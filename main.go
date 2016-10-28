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
	flushSize     = kingpin.Flag("flush-size", "Maximum number of records to buffer between insertAll calls.").Default("50").Int()
	ignoreUnknown = kingpin.Flag("ignore-unknown", "Accept values that don't match the schema. By default records with non-matching schemas will be rejected and inserts will fail.").Default("false").Bool()
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
	reader := bufio.NewReaderSize(os.Stdin, 20*1024)
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

	config := &bigquery.InserterOpts{
		Destination:    destination,
		Identity:       identity(),
		IgnoreUnknowns: *ignoreUnknown,
		FlushSize:      *flushSize,
	}
	inserter, err := bigquery.NewInserter(config)
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

	flushOrExit(inserter)
	fmt.Printf("Inserted %d rows\n", inserter.InsertedRows())
}

func flushOrExit(inserter *bigquery.Inserter) {
	err := inserter.Flush()
	if err != nil {
		fmt.Println("ERROR:", err.Error())
		os.Exit(1)
	}
}

func flusher(inserter *bigquery.Inserter) {
	for _ = range time.Tick(*flushInterval) {
		flushOrExit(inserter)
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
