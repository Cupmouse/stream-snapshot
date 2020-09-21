package main

import (
	"fmt"
	"testing"

	"github.com/aws/aws-lambda-go/events"
)

func makeLambdaEvent(exchange string, channels []string, nanosec string, format string) events.APIGatewayProxyRequest {
	return events.APIGatewayProxyRequest{
		PathParameters: map[string]string{
			"exchange": exchange,
			"nanosec":  nanosec,
		},
		QueryStringParameters: map[string]string{
			"format": format,
		},
		MultiValueQueryStringParameters: map[string][]string{
			"channels": channels,
		},
		Headers: map[string]string{"Authorization": "Bearer demo"},
	}
}

func testCommon(t *testing.T, res *events.APIGatewayProxyResponse, err error) {
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Body) == 0 {
		t.Fatal("empty body")
	}
	if res.StatusCode != 200 {
		t.Fatal(res.Body)
	}
	fmt.Println(res.Body)
}

func TestBitmexOrderBookL2(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitmex", []string{"orderBookL2_XBTUSD", "orderBookL2_thisdoesnotexist", "orderBookL2"}, "1598941025555000000", "json"))
	testCommon(t, res, err)
}

func TestBitfinexBook(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitfinex", []string{"book_tBTCUSD"}, "1598941025555000000", "json"))
	testCommon(t, res, err)
}

func TestBinanceDepth(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("binance", []string{"btcusdt@depth@100ms"}, "1598941025555000000", "json"))
	testCommon(t, res, err)
}

func TestBinanceDepthRest(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("binance", []string{"btcusdt@rest_depth"}, "1598941025555000000", "json"))
	testCommon(t, res, err)
}

func TestBitflyerLightningBoard(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitflyer", []string{"lightning_board_BTC_JPY"}, "1598941025555000000", "json"))
	testCommon(t, res, err)
}

func TestBitflyerLightningSnapshot(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("bitflyer", []string{"lightning_board_snapshot_BTC_JPY"}, "1598941025555000000", "json"))
	testCommon(t, res, err)
}

func TestLiquidPriceLaddersCashBTCJPY(t *testing.T) {
	res, err := handleRequest(makeLambdaEvent("liquid", []string{"price_ladders_cash_btcjpy_buy"}, "1598941025555000000", "json"))
	testCommon(t, res, err)
}
