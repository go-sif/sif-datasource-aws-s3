package s3

import (
	"bytes"
	"fmt"
	"io"
	"log"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/errors"
)

type s3PartitionIterator struct {
	downloadChan    <-chan *downloadedFile
	currentIterator sif.PartitionIterator
	finished        bool
	source          *DataSource
	schema          sif.Schema
}

func (pi *s3PartitionIterator) HasNextPartition() bool {
	return !pi.finished
}

// if unlockPartition is not nil, it must be called when one is finished with the returned Partition
func (pi *s3PartitionIterator) NextPartition() (part sif.Partition, unlockPartition func(), err error) {
	// if we have a current iterator from a parser, use it
	if pi.currentIterator != nil && pi.currentIterator.HasNextPartition() {
		next, done, err := pi.currentIterator.NextPartition()
		if err == nil {
			return next, done, nil
		} else if _, ok := err.(errors.NoMorePartitionsError); !ok {
			return nil, nil, err
		}
	}
	// otherwise fetch more data and start parsing it
	file, ok := <-pi.downloadChan
	if !ok {
		pi.finished = true
		return nil, nil, errors.NoMorePartitionsError{}
	}
	log.Printf("Parsing file %s into partitions", file.key)
	var reader io.Reader
	if pi.source.conf.Decoder != nil {
		buf, err := pi.source.conf.Decoder(file.data)
		if err != nil {
			return nil, nil, fmt.Errorf("WARNING: couldn't decode buffer for downloaded file %s: %e", file.key, err)
		}
		reader = bytes.NewReader(buf)
	} else {
		reader = bytes.NewReader(file.data)
	}
	ppi, err := pi.source.parser.Parse(reader, pi.source, pi.source.schema, pi.schema, func() {})
	if err != nil {
		return nil, nil, err
	}
	pi.currentIterator = ppi
	return pi.NextPartition()
}

func (pi *s3PartitionIterator) OnEnd(onEnd func()) {
	// do nothing
}
