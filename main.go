package main

import (
	"fmt"
	"os"
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

	// get parameters
	c, serr := makeContext(event)
	if serr != nil {
		response = sc.MakeResponse(400, serr.Error())
		return
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
	if apikey.Demo && Production {
		// if this apikey is demo key, then check if nanosec is in allowed range
		if c.nanosec < int64(streamcommons.DemoAPIKeyAllowedStart) || c.nanosec >= int64(streamcommons.DemoAPIKeyAllowedEnd) {
			response = sc.MakeResponse(400, "'nanosec' is out of range: You are using demo API-key")
			return
		}
	}
	fmt.Printf("setup end : %d\n", time.Now().Sub(st))
	fmt.Printf("snapshot start : %d\n", time.Now().Sub(st))
	// write snapshot
	result, scanned, serr := snapshot(c)
	if serr != nil {
		err = fmt.Errorf("snapshot: %v", serr)
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

func main() {
	lambda.Start(handleRequest)
}
