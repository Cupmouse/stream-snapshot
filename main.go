package main

import (
	"bytes"
	"database/sql"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/exchangedataset/streamcommons"
	sc "github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/formatter"
	"github.com/exchangedataset/streamcommons/simulator"
)

func handleRequest(event events.APIGatewayProxyRequest) (response *events.APIGatewayProxyResponse, err error) {
	var db *sql.DB
	db, err = sc.ConnectDatabase()
	if err != nil {
		return
	}
	defer func() {
		serr := db.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%+v, original error was: %+v", serr, err)
			} else {
				err = serr
			}
		}
	}()

	st := time.Now()
	// initialize apikey
	apikey, cerr := sc.NewAPIKey(event)
	if cerr != nil {
		fmt.Printf("%+v", cerr)
		response = sc.MakeResponse(401, fmt.Sprintf("API-key authorization failed"))
		return
	}
	if !apikey.Demo {
		// check API-key if valid
		cerr = apikey.CheckAvalability(db)
		if cerr != nil {
			fmt.Printf("%+v", cerr)
			response = sc.MakeResponse(401, fmt.Sprintf("API key is invalid: %s", cerr.Error()))
			return
		}
	}
	fmt.Printf("apikey checked : %d\n", time.Now().Sub(st))
	// get parameters
	exchange, ok := event.PathParameters["exchange"]
	if !ok {
		response = sc.MakeResponse(400, "'exchange' must be specified")
		return
	}
	nanosecStr, ok := event.PathParameters["nanosec"]
	if !ok {
		response = sc.MakeResponse(400, "'nanosec' must be specified")
		return
	}
	nanosec, cerr := strconv.ParseInt(nanosecStr, 10, 64)
	if cerr != nil {
		response = sc.MakeResponse(400, "'nanosec' must be of integer type")
		return
	}
	minute := nanosec / 60 / 1000000000
	channels, ok := event.MultiValueQueryStringParameters["channels"]
	if !ok {
		response = sc.MakeResponse(400, "'channels' must be specified")
		return
	}
	format, ok := event.QueryStringParameters["format"]
	if !ok {
		// default format is raw
		format = "raw"
	}
	if apikey.Demo {
		// if this apikey is demo key, then check if nanosec is in allowed range
		if nanosec < int64(streamcommons.DemoAPIKeyAllowedStart) || nanosec >= int64(streamcommons.DemoAPIKeyAllowedEnd) {
			response = sc.MakeResponse(400, "'nanosec' is out of range: You are using demo API-key")
			return
		}
	}
	var form formatter.Formatter
	if format != "raw" {
		// check if it has the right formatter for this exhcange and format
		form, cerr = formatter.GetFormatter(exchange, channels, format)
		if cerr != nil {
			response = sc.MakeResponse(400, cerr.Error())
			return
		}
	}
	// check if it has the right simulator for this request
	sim, cerr := simulator.GetSimulator(exchange, channels)
	if cerr != nil {
		response = sc.MakeResponse(400, cerr.Error())
		return
	}
	fmt.Printf("setup end : %d\n", time.Now().Sub(st))
	// list dataset to read to reconstruct snapshot
	// and make response string
	var rows *sql.Rows
	rows, err = db.Query("CALL dataset_info.find_dataset_for_snapshot(?, ?)", exchange, minute)
	if err != nil {
		return
	}
	defer func() {
		cerr := rows.Close()
		if cerr != nil {
			if err != nil {
				err = fmt.Errorf("%+v, orignal error was %+v", cerr, err)
			} else {
				err = cerr
			}
		}
	}()
	var totalScanned int64
	var key string
	for rows.Next() {
		err = rows.Scan(&key)
		fmt.Printf("reading start for file %s : %d\n", key, time.Now().Sub(st))

		if err != nil {
			return
		}
		var scanned int64
		var stop bool
		scanned, stop, err = downloadAndFeed(key, nanosec, channels, sim)
		if err != nil {
			return
		}
		totalScanned += scanned
		if stop {
			// it is enough to make snapshot
			break
		}
	}
	fmt.Printf("snapshot start : %d\n", time.Now().Sub(st))

	// write snapshot
	buf := make([]byte, 0, 10*1024*1024)
	buffer := bytes.NewBuffer(buf)
	var snapshots []simulator.Snapshot
	snapshots, err = sim.TakeSnapshot()
	if err != nil {
		return
	}
	for _, snapshot := range snapshots {
		var out [][]byte
		if form != nil {
			// if formatter is specified, write formatted
			out, err = form.Format(snapshot.Channel, snapshot.Snapshot)
			if err != nil {
				return
			}
		} else {
			out = [][]byte{snapshot.Snapshot}
		}
		for _, str := range out {
			_, err = buffer.WriteString(fmt.Sprintf("%d\t%s\t", nanosec, snapshot.Channel))
			if err != nil {
				return
			}
			_, err = buffer.Write(str)
			if err != nil {
				return
			}
			_, err = buffer.WriteRune('\n')
			if err != nil {
				return
			}
		}
	}

	fmt.Printf("snapshot end : %d\n", time.Now().Sub(st))

	result := buffer.Bytes()
	var incremented int64
	if apikey.Demo {
		incremented = streamcommons.CalcQuotaUsed(totalScanned)
	} else {
		incremented, err = apikey.IncrementUsed(db, totalScanned)
		if err != nil {
			return
		}
	}
	fmt.Printf("increment transfer end : %d\n", time.Now().Sub(st))
	// return result
	var returnCode int
	if totalScanned == 0 {
		returnCode = 404
	} else {
		returnCode = 200
	}
	return sc.MakeLargeResponse(returnCode, result, incremented)
}

func main() {
	lambda.Start(handleRequest)
}
