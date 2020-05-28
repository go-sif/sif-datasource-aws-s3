package s3

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-sif/sif"
)

type downloadedFile struct {
	key  string
	data []byte
}

// PartitionLoader is capable of loading partitions of data from a file
type PartitionLoader struct {
	keys         []string
	source       *DataSource
	downloader   *s3manager.Downloader
	downloadChan chan *downloadedFile
}

func (pl *PartitionLoader) asyncDownload() {
	pl.downloader = s3manager.NewDownloader(pl.source.conf.Session)
	req := &s3.GetObjectInput{
		Bucket:       aws.String(pl.source.conf.Bucket),
		RequestPayer: aws.String(pl.source.conf.RequestPayer),
		Key:          nil,
	}
	for _, k := range pl.keys {
		if pl.source.conf.Filter != nil && !pl.source.conf.Filter.MatchString(k) {
			continue
		}
		buff := &aws.WriteAtBuffer{}
		req.Key = aws.String(k)
		nbytes, err := pl.downloader.Download(buff, req)
		if err != nil {
			panic(err)
		}
		log.Printf("Downloaded file %s with size %d", k, nbytes)
		pl.downloadChan <- &downloadedFile{
			key:  k,
			data: buff.Bytes(),
		}
	}
	close(pl.downloadChan)
}

// ToString returns a string representation of this PartitionLoader
func (pl *PartitionLoader) ToString() string {
	return fmt.Sprintf("S3 loader filenames: %s", pl.keys)
}

// Load is capable of loading partitions of data from a file
func (pl *PartitionLoader) Load(parser sif.DataSourceParser, widestInitialSchema sif.Schema) (sif.PartitionIterator, error) {
	if pl.downloadChan == nil {
		pl.downloadChan = make(chan *downloadedFile, pl.source.conf.PrefetchLimit)
		go pl.asyncDownload()
	}
	return &s3PartitionIterator{
		downloadChan: pl.downloadChan,
		source:       pl.source,
		schema:       widestInitialSchema,
	}, nil
}

// GobEncode serializes a PartitionLoader
func (pl *PartitionLoader) GobEncode() ([]byte, error) {
	buff := new(bytes.Buffer)
	e := gob.NewEncoder(buff)
	err := e.Encode(pl.keys)
	if err != nil {
		return nil, err
	}
	return buff.Bytes(), nil
}

// GobDecode deserializes a PartitionLoader
func (pl *PartitionLoader) GobDecode(in []byte) error {
	var deser []string
	buff := bytes.NewBuffer(in)
	d := gob.NewDecoder(buff)
	err := d.Decode(&deser)
	if err != nil {
		return err
	}
	pl.keys = deser
	return nil
}
