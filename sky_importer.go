package main

import (
	"bufio"
	"compress/gzip"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"github.com/skydb/sky.go"
	"io"
	"os"
	"path/filepath"
	"time"
)

//------------------------------------------------------------------------------
//
// Constants
//
//------------------------------------------------------------------------------

const (
	Version = "0.3.0"
)

const (
	defaultHost      = "localhost"
	defaultPort      = 8585
	defaultOverwrite = false
	defaultVerbose   = false
)

const (
	hostUsage      = "the host the Sky server is running on"
	portUsage      = "the port the Sky server is running on"
	tableNameUsage = "the table to insert events into"
	overwriteUsage = "overwrite an existing table if one exists"
	verboseUsage   = "verbose logging"
)

//------------------------------------------------------------------------------
//
// Variables
//
//------------------------------------------------------------------------------

var host string
var port int
var tableName string
var overwrite bool
var verbose bool

//------------------------------------------------------------------------------
//
// Functions
//
//------------------------------------------------------------------------------

//--------------------------------------
// Initialization
//--------------------------------------

func init() {
	flag.StringVar(&host, "host", defaultHost, hostUsage)
	flag.StringVar(&host, "h", defaultHost, hostUsage+" (shorthand)")
	flag.IntVar(&port, "port", defaultPort, portUsage)
	flag.IntVar(&port, "p", defaultPort, portUsage+" (shorthand)")
	flag.StringVar(&tableName, "table", "", tableNameUsage)
	flag.StringVar(&tableName, "t", "", tableNameUsage+" (shorthand)")
	flag.BoolVar(&overwrite, "overwrite", defaultOverwrite, overwriteUsage)
	flag.BoolVar(&verbose, "v", defaultVerbose, verboseUsage)
	flag.BoolVar(&verbose, "verbose", defaultVerbose, verboseUsage)
}

//--------------------------------------
// Main
//--------------------------------------

func main() {
	var err error
	flag.Parse()

	// Make sure we have files.
	if tableName == "" {
		usage()
	}

	// Setup the client and table.
	_, table, err := setup()
	if err != nil {
		warn("%v", err)
		os.Exit(1)
	}

	if flag.NArg() > 0 {
		// Loop over files.
		for _, filename := range flag.Args() {
			if err = importFile(table, filename); err != nil {
				warn("Invalid file: %v", err)
			}
		}
	} else {
		warn("waiting for stdin...")
		importStdin(table)
	}
}

func usage() {
	warn("usage: sky-importer [OPTIONS] FILE or STDIN")
	os.Exit(1)
}

//--------------------------------------
// Setup
//--------------------------------------

func setup() (*sky.Client, *sky.Table, error) {
	warn("Connecting to %s:%d.\n", host, port)

	// Create a Sky client.
	client := sky.NewClient(host)
	client.Port = port

	// Check if the server is running.
	if !client.Ping() {
		return nil, nil, errors.New("Server is not running.")
	}

	// Check if the table exists first.
	table, err := client.GetTable(tableName)
	if table == nil {
		warn("Unable to find table '%v': %v", tableName, err)
		os.Exit(1)
	}

	return client, table, nil
}

//--------------------------------------
// Import
//--------------------------------------

// Imports indefinitely from STDIN
func importStdin(table *sky.Table) {
	reader := bufio.NewReader(os.Stdin)

	lineNumber := 1
	for {
		inputBytes, _, err := reader.ReadLine()
		if err != nil {
			warn("%v", err)
		}
		data := map[string]interface{}{}
		err = json.Unmarshal(inputBytes, &data)
		if err != nil {
			warn("%v", err)
		}

		if err = importData(table, data); err != nil {
			warn("[L%d] %v", lineNumber, err)
		}

		lineNumber++
	}
}

// Imports a single JSON formatted file into a table.
func importFile(table *sky.Table, filename string) error {
	// Open the file.
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	// Wrap it in a buffer.
	var reader io.Reader

	// If this is a gzipped file then decompress it.
	if filepath.Ext(filename) == ".gz" {
		gzipReader, err := gzip.NewReader(file)
		if err != nil {
			return err
		}
		defer gzipReader.Close()
		reader = gzipReader
	} else {
		reader = bufio.NewReader(file)
	}

	// Decode JSON and insert.
	lineNumber := 1
	decoder := json.NewDecoder(reader)
	for {
		data := map[string]interface{}{}
		if err = decoder.Decode(&data); err == io.EOF {
			break
		} else if err != nil {
			warn("Invalid JSON on line %d")
		} else {
			if err = importData(table, data); err != nil {
				warn("[L%d] %v", lineNumber, err)
			}
		}
		lineNumber++
	}

	return nil
}

// Imports a single root object parsed from the JSON file.
func importData(table *sky.Table, data map[string]interface{}) error {
	var err error
	if data == nil {
		return errors.New("Null object cannot be imported")
	}

	// Convert object id to string.
	if data["id"] == nil {
		return errors.New("Invalid ID")
	} else if data["id"] == "" {
		return errors.New("ID cannot be blank")
	}
	objectId := fmt.Sprintf("%v", data["id"])
	delete(data, "id")

	// Parse timestamp.
	var timestamp time.Time
	if timestampString, ok := data["timestamp"].(string); ok {
		if timestamp, err = time.Parse(time.RFC3339, timestampString); err != nil {
			return errors.New("Invalid timestamp")
		}
	}
	delete(data, "timestamp")

	// Create Sky event.
	event := sky.NewEvent(timestamp, data)
	return table.AddEvent(objectId, event, sky.Merge)
}

//--------------------------------------
// Utility
//--------------------------------------

// Writes to standard error.
func warn(msg string, v ...interface{}) {
	fmt.Fprintf(os.Stderr, msg+"\n", v...)
}
