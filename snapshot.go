package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"strconv"
	"time"
	"unsafe"

	"github.com/exchangedataset/streamcommons"
)

func feedToSimulator(reader *bufio.Reader, c *SnapshotContext) (scanned int, stop bool, err error) {
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
			if timestamp > c.nanosec {
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
				err = c.sim.ProcessMessageChannelKnown(channelTrimmed, line)
			} else if typeStr == "state\t" {
				err = c.sim.ProcessState(channelTrimmed, line)
			}
			tprocess += time.Now().Sub(st).Nanoseconds()
			if err != nil {
				return
			}
			continue
		} else if typeStr == "start\t" {
			url, serr := reader.ReadBytes('\n')
			if serr != nil {
				return 0, false, serr
			}
			scanned += len(url)
			err = c.setNewSimulator()
			if err != nil {
				return
			}
			st := time.Now()
			err = c.sim.ProcessStart(url)
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

func prepareReaderAndFeed(reader io.ReadCloser, c *SnapshotContext) (scanned int, stop bool, err error) {
	defer func() {
		serr := reader.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
			return
		}
	}()
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
				err = fmt.Errorf("%v, original error was: %v", serr, err)
			} else {
				err = serr
			}
			return
		}
	}()
	breader := bufio.NewReader(greader)
	scanned, stop, err = feedToSimulator(breader, c)
	return
}

func snapshot(c *SnapshotContext) (ret []byte, totalScanned int, err error) {
	st := time.Now()

	// list dataset to read to reconstruct snapshot
	// and make response string
	ctx := context.Background()
	tenMinute := (c.minute / 10) * 10
	keys := make([]string, c.minute-tenMinute+1)
	for i := int64(0); i <= c.minute-tenMinute; i++ {
		keys[i] = fmt.Sprintf("%s_%d.gz", c.exchange, tenMinute+i)
	}
	fmt.Printf("keys: %v\n", keys)
	bodies := streamcommons.S3GetAll(ctx, keys)
	defer func() {
		serr := bodies.Close()
		if serr != nil {
			if err != nil {
				err = fmt.Errorf("snapshot: bodies close: %v, originally: %v", serr, err)
			} else {
				err = fmt.Errorf("snapshot: bodies close: %v", serr)
			}
		}
	}()

	i := 0
	for {
		body, ok := bodies.Next()
		if !ok {
			break
		}
		if body == nil {
			fmt.Printf("skipping file %d: did not exist\n", i)
			continue
		}
		fmt.Printf("reading file %d : %d\n", i, time.Now().Sub(st))
		scanned, stop, serr := prepareReaderAndFeed(body, c)
		totalScanned += scanned
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
	buf := make([]byte, 0, 10*1024*1024)
	buffer := bytes.NewBuffer(buf)
	snapshots, serr := c.sim.TakeSnapshot()
	if serr != nil {
		err = serr
		return
	}
	for _, snapshot := range snapshots {
		if c.form != nil {
			// if formatter is specified, write formatted
			formatted, serr := c.form.FormatMessage(snapshot.Channel, snapshot.Snapshot)
			if serr != nil {
				err = serr
				return
			}
			for _, f := range formatted {
				nanosecStr := strconv.FormatInt(c.nanosec, 10)
				if _, err = buffer.WriteString(nanosecStr); err != nil {
					return
				}
				if _, err = buffer.WriteRune('\t'); err != nil {
					return
				}
				if _, err = buffer.WriteString(f.Channel); err != nil {
					return
				}
				if _, err = buffer.WriteRune('\t'); err != nil {
					return
				}
				if _, err = buffer.Write(f.Message); err != nil {
					return
				}
				if _, err = buffer.WriteRune('\n'); err != nil {
					return
				}
			}
		} else {
			nanosecStr := strconv.FormatInt(c.nanosec, 10)
			if _, err = buffer.WriteString(nanosecStr); err != nil {
				return
			}
			if _, err = buffer.WriteRune('\t'); err != nil {
				return
			}
			if _, err = buffer.WriteString(snapshot.Channel); err != nil {
				return
			}
			if _, err = buffer.WriteRune('\t'); err != nil {
				return
			}
			if _, err = buffer.Write(snapshot.Snapshot); err != nil {
				return
			}
			if _, err = buffer.WriteRune('\n'); err != nil {
				return
			}
		}
	}
	ret = buffer.Bytes()
	return
}
