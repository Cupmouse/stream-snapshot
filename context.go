package main

import (
	"errors"
	"fmt"
	"strconv"

	"github.com/aws/aws-lambda-go/events"
	"github.com/exchangedataset/streamcommons/formatter"
	"github.com/exchangedataset/streamcommons/simulator"
)

// SnapshotContext is the context and parameter for snapshot
type SnapshotContext struct {
	exchange string
	nanosec  int64
	minute   int64
	channels []string
	format   string
	sim      simulator.Simulator
	form     formatter.Formatter
}

func (p *SnapshotContext) setNewSimulator() error {
	simChannels := simulator.ToSimulatorChannel(p.exchange, p.channels)
	sim, serr := simulator.GetSimulator(p.exchange, simChannels)
	if serr != nil {
		return serr
	}
	p.sim = sim
	return nil
}

func (p *SnapshotContext) initFormatter() (err error) {
	if p.format != "raw" {
		p.form, err = formatter.GetFormatter(p.exchange, p.channels, p.format)
		if err != nil {
			err = fmt.Errorf("getFormatter: %v", err)
		}
	}
	return
}

func makeContext(event events.APIGatewayProxyRequest) (c *SnapshotContext, err error) {
	c = new(SnapshotContext)

	var ok bool
	c.exchange, ok = event.PathParameters["exchange"]
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
	c.nanosec, serr = strconv.ParseInt(nanosecStr, 10, 64)
	if serr != nil {
		err = errors.New("'nanosec' must be of integer type")
		return
	}
	c.minute = c.nanosec / 60 / 1000000000
	c.channels, ok = event.MultiValueQueryStringParameters["channels"]
	if !ok {
		err = errors.New("'channels' must be specified")
		return
	}
	c.format, ok = event.QueryStringParameters["format"]
	if !ok {
		// default format is raw
		c.format = "raw"
	}
	// check if it has the right simulator for this request
	err = c.setNewSimulator()
	if err != nil {
		return
	}
	err = c.initFormatter()
	return
}
