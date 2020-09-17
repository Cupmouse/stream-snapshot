package main

import (
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
	fmt.Printf("setup end : %d\n", time.Now().Sub(st))
	// list dataset to read to reconstruct snapshot
	// and make response string
	ctx := context.Background()
	tenMinute := (param.minute / 10) * 10
	keys := make([]string, param.minute-tenMinute+1)
	for i := int64(0); i <= param.minute-tenMinute; i++ {
		keys[i] = fmt.Sprintf("%s_%d.gz", param.exchange, tenMinute+i)
	}
	fmt.Printf("keys: %v\n", keys)
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
	fmt.Printf("snapshot start : %d\n", time.Now().Sub(st))
	// write snapshot
	result, scanned, eerr, serr := snapshot(param, bodies)
	if serr != nil {
		err = fmt.Errorf("snapshot: %v", serr)
		return
	}
	if eerr != nil {
		response = sc.MakeResponse(400, eerr.Error())
		return
	}
	fmt.Printf("snapshot end : %d\n", time.Now().Sub(st))
	var incremented int64
	if apikey.Demo {
		incremented = streamcommons.CalcQuotaUsed(scanned)
	} else {
		incremented, err = apikey.IncrementUsed(db, scanned)
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
	postFilter, ok := event.MultiValueQueryStringParameters["postFilter"]
	if ok {
		param.postFilter = make(map[string]bool)
		for _, ch := range postFilter {
			param.postFilter[ch] = true
		}
	}
	return
}

func main() {
	lambda.Start(handleRequest)
}
