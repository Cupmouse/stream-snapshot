package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/aws/aws-lambda-go/events"
	"github.com/aws/aws-lambda-go/lambda"

	"github.com/exchangedataset/streamcommons"
	sc "github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/formatter"
	"github.com/exchangedataset/streamcommons/simulator"
)

// Production is `true` if and only if this instance is running on the context of production environment.
var Production = os.Getenv("PRODUCTION") == "1"

func handleRequest(event events.APIGatewayProxyRequest) (response *events.APIGatewayProxyResponse, err error) {
	if Production {
		sc.AWSEnableProduction()
	}

	db, serr := sc.ConnectDatabase()
	if serr != nil {
		err = serr
		return
	}
	defer func() {
		serr := db.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	st := time.Now()
	// initialize apikey
	apikey, serr := sc.NewAPIKey(event)
	if serr != nil {
		fmt.Printf("%v", serr)
		response = sc.MakeResponse(401, fmt.Sprintf("API-key authorization failed"))
		return
	}
	if !apikey.Demo {
		// check API-key if valid
		serr = apikey.CheckAvalability(db)
		if serr != nil {
			fmt.Printf("%v", serr)
			response = sc.MakeResponse(401, fmt.Sprintf("API key is invalid: %v", serr))
			return
		}
	}
	fmt.Printf("apikey checked : %d\n", time.Now().Sub(st))
	// get parameters
	param, serr := makeParameter(event)
	if serr != nil {
		response = sc.MakeResponse(400, serr.Error())
		return
	}
	if apikey.Demo && Production {
		// if this apikey is demo key, then check if nanosec is in allowed range
		if param.nanosec < int64(streamcommons.DemoAPIKeyAllowedStart) || param.nanosec >= int64(streamcommons.DemoAPIKeyAllowedEnd) {
			response = sc.MakeResponse(400, "'nanosec' is out of range: You are using demo API-key")
			return
		}
	}
	var form formatter.Formatter
	if param.format != "raw" {
		// check if it has the right formatter for this exhcange and format
		form, serr = formatter.GetFormatter(param.exchange, param.channels, param.format)
		if serr != nil {
			response = sc.MakeResponse(400, serr.Error())
			return
		}
	}
	// check if it has the right simulator for this request
	sim, serr := simulator.GetSimulator(param.exchange, param.channels)
	if serr != nil {
		response = sc.MakeResponse(400, serr.Error())
		return
	}
	fmt.Printf("setup end : %d\n", time.Now().Sub(st))
	// list dataset to read to reconstruct snapshot
	// and make response string
	ctx := context.Background()
	tenMinute := (param.minute / 10) * 10
	keys := make([]string, param.minute-tenMinute+1)
	for i := int64(0); i <= param.minute-tenMinute; i++ {
		keys[i] = fmt.Sprintf("%s_%d.gz", param.exchange, tenMinute+i)
	}
	bodies := sc.S3GetAll(ctx, keys)
	defer func() {
		serr := bodies.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("snapshot: bodies close: %v, originally: %v", serr, err)
			} else {
				err = serr
			}
		}
	}()
	var totalScanned int64
	i := 0
	for {
		body, ok := bodies.Next()
		if !ok {
			break
		}
		if body == nil {
			fmt.Printf("skipping file %s: did not exist: %d:\n", keys[i], time.Now().Sub(st))
			continue
		}
		fmt.Printf("reading file %s : %d\n", keys[i], time.Now().Sub(st))
		scanned, stop, serr := feed(body, param.nanosec, param.channels, sim)
		totalScanned += int64(scanned)
		if serr != nil {
			err = serr
			return
		}
		if stop {
			// it is enough to make snapshot
			break
		}
		i++
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
			out, err = form.FormatMessage(snapshot.Channel, snapshot.Snapshot)
			if err != nil {
				return
			}
		} else {
			out = [][]byte{snapshot.Snapshot}
		}
		for _, str := range out {
			_, err = buffer.WriteString(fmt.Sprintf("%d\t%s\t", param.nanosec, snapshot.Channel))
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
	if len(result) == 0 {
		returnCode = 404
	} else {
		returnCode = 200
	}
	return sc.MakeLargeResponse(returnCode, result, incremented)
}

func makeParameter(event events.APIGatewayProxyRequest) (param SnapshotParameter, err error) {
	var ok bool
	param.exchange, ok = event.PathParameters["exchange"]
	if !ok {
		err = errors.New("'exchange' must be specified")
		return
	}
	nanosecStr, ok := event.PathParameters["nanosec"]
	if !ok {
		err = errors.New("'nanosec' must be specified")
		return
	}
	var serr error
	param.nanosec, serr = strconv.ParseInt(nanosecStr, 10, 64)
	if serr != nil {
		err = errors.New("'nanosec' must be of integer type")
		return
	}
	param.minute = param.nanosec / 60 / 1000000000
	param.channels, ok = event.MultiValueQueryStringParameters["channels"]
	if !ok {
		err = errors.New("'channels' must be specified")
		return
	}
	param.format, ok = event.QueryStringParameters["format"]
	if !ok {
		// default format is raw
		param.format = "raw"
	}
	return
}

func main() {
	lambda.Start(handleRequest)
}
