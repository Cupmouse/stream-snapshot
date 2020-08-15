package main

import (
	"bufio"
	"compress/gzip"
	"fmt"
	"io"
	"strconv"
	"time"
	"unsafe"

	sc "github.com/exchangedataset/streamcommons"
	"github.com/exchangedataset/streamcommons/simulator"
)

func feedToSimulator(reader *bufio.Reader, targetNanosec int64, sim simulator.Simulator) (scanned int, stop bool, err error) {
	tprocess := int64(0)
	for {
		// read type str
		typeBytes, serr := reader.ReadBytes('\t')
		if serr != nil {
			if serr == io.EOF {
				break
			} else {
				// some error
				err = serr
				return
			}
		}
		scanned += len(typeBytes)
		typeStr := *(*string)(unsafe.Pointer(&typeBytes))
		// read timestamp
		var timestampBytes []byte
		if typeStr == "end\t" {
			timestampBytes, err = reader.ReadBytes('\n')
		} else {
			timestampBytes, err = reader.ReadBytes('\t')
		}
		if err != nil {
			return
		}
		scanned += len(timestampBytes)
		if typeStr != "state\t" {
			timestampStr := *(*string)(unsafe.Pointer(&timestampBytes))
			// remove the last character on timestampStr because it is TAB
			var timestamp int64
			timestamp, err = strconv.ParseInt(timestampStr[:len(timestampStr)-1], 10, 64)
			if err != nil {
				return
			}
			if timestamp >= targetNanosec {
				// lines after the target time is not needed to construct a snapshot
				// unless it is not a state line
				// state lines should be considered when the target time is before status lines
				// but it have not read first dataset to know the "initial state"
				stop = true
				return
			}
		}
		if typeStr == "msg\t" || typeStr == "state\t" {
			// get channel
			var channelBytes []byte
			channelBytes, err = reader.ReadBytes('\t')
			if err != nil {
				return
			}
			scanned += len(channelBytes)
			channelTrimmedBytes := channelBytes[:len(channelBytes)-1]
			channelTrimmed := *(*string)(unsafe.Pointer(&channelTrimmedBytes))
			// should this channel be passed to simulator?
			var line []byte
			line, err = reader.ReadBytes('\n')
			if err != nil {
				return
			}
			scanned += len(line)
			st := time.Now()
			if typeStr == "msg\t" {
				_, err = sim.ProcessMessageChannelKnown(channelTrimmed, line)
			} else if typeStr == "state\t" {
				err = sim.ProcessState(channelTrimmed, line)
			}
			tprocess += time.Now().Sub(st).Nanoseconds()
			if err != nil {
				return
			}
			continue
		} else if typeStr == "start\t" {
			url, serr := reader.ReadBytes('\n')
			if serr != nil {
				return serr
			}
			scanned += len(url)
			st := time.Now()
			_, err = sim.ProcessStart(url)
			tprocess += time.Now().Sub(st).Nanoseconds()
			if err != nil {
				return
			}
			continue
		}

		// ignore this line
		var skipped []byte
		skipped, err = reader.ReadBytes('\n')
		scanned += len(skipped)
		if err != nil {
			return
		}
	}
	fmt.Printf("total processing time : %d\n", tprocess)
	return
}

func downloadAndFeed(key string, targetNanosec int64, channels []string, sim simulator.Simulator) (scanned int64, stop bool, err error) {
	st := time.Now()
	reader, err := sc.GetS3Object(key)
	if err != nil {
		return
	}
	defer func() {
		serr := reader.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%+v, original error was: %+v", serr, err)
			} else {
				err = serr
			}
			return
		}
	}()
	fmt.Printf("got response : %d\n", time.Now().Sub(st))
	var greader *gzip.Reader
	greader, err = gzip.NewReader(reader)
	if err != nil {
		return
	}
	// to ensure closing readers
	defer func() {
		serr := greader.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%+v, original error was: %+v", serr, err)
			} else {
				err = serr
			}
			return
		}
	}()
	breader := bufio.NewReader(greader)

	fmt.Printf("reader setup : %d\n", time.Now().Sub(st))
	var datasetScanned int
	datasetScanned, stop, err = feedToSimulator(breader, targetNanosec, sim)
	scanned = int64(datasetScanned)
	return
}
