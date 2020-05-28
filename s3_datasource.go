package s3

import (
	"log"
	"regexp"

	"github.com/aws/aws-sdk-go/aws/session"

	"github.com/go-sif/sif"
	"github.com/go-sif/sif/datasource"
)

// DataSource is a set of files in an s3 bucket, containing data which will be manipulating according to a DataFrame
type DataSource struct {
	conf   *DataSourceConf
	schema sif.Schema
	parser sif.DataSourceParser
}

// DataSourceConf configures a file DataSource
type DataSourceConf struct {
	Bucket string
	// Prefix limits the response to keys prefixed by this string
	Prefix       string
	Filter       *regexp.Regexp
	RequestPayer string
	// KeyBatchSize must be less than 1000 and represents the number of documents which will
	// be assigned as a batch to a Sif worker at one time. Files are assigned in batches
	// so that workers can download and parse files concurrently.
	KeyBatchSize int64
	// PrefetchLimit is a limit on the number of files which workers will prefetch and store in memory
	PrefetchLimit int
	Session       *session.Session
	Decoder       func([]byte) ([]byte, error)
}

// CreateDataFrame is a factory for DataSources
func CreateDataFrame(conf *DataSourceConf, parser sif.DataSourceParser, schema sif.Schema) sif.DataFrame {
	if conf.PrefetchLimit == 0 {
		conf.PrefetchLimit = 2
	}
	if conf.KeyBatchSize == 0 {
		conf.KeyBatchSize = 1000
	}
	if conf.Session == nil {
		log.Fatalf("DataSourceConf.Session must be an aws session.Session, but was nil")
	}
	if len(conf.Bucket) == 0 {
		log.Fatalf("DataSourceConf.Bucket must be a bucket name")
	}
	source := &DataSource{conf: conf, parser: parser, schema: schema}
	df := datasource.CreateDataFrame(source, parser, schema)
	return df
}

// Analyze returns a PartitionMap, describing how the source file will be divided into Partitions
func (fs *DataSource) Analyze() (sif.PartitionMap, error) {
	return newPartitionMap(fs)
}

// DeserializeLoader creates a PartitionLoader for this DataSource from a serialized representation
func (fs *DataSource) DeserializeLoader(bytes []byte) (sif.PartitionLoader, error) {
	pl := PartitionLoader{keys: nil, source: fs}
	err := pl.GobDecode(bytes)
	if err != nil {
		return nil, err
	}
	return &pl, nil
}

// IsStreaming returns true iff this DataSource provides a continuous stream of data
func (fs *DataSource) IsStreaming() bool {
	return false
}
