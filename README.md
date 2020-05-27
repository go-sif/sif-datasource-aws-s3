# Sif S3 DataSource

An AWS S3 DataSource for Sif.

```bash
$ go get github.com/go-sif/sif-datasource-aws-s3@master
$ go get github.com/aws/aws-sdk-go
```

## Usage

1. Create a `Schema` which represents the fields you intend to extract from each document in the target index:

```go
import (
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/schema"
)

schema := schema.CreateSchema()
schema.CreateColumn("coords.x", &sif.Float64ColumnType{})
schema.CreateColumn("coords.z", &sif.Float64ColumnType{})
schema.CreateColumn("date", &sif.TimeColumnType{Format: "2006-01-02 15:04:05"})
```

2. Create an AWS Session with your desired configuration parameters

```go
import (
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/schema"
	"github.com/aws/aws-sdk-go/aws/session"
)

// ...

sess := session.Must(session.NewSession())
```

3. Finally, define your configuration and create a `DataFrame` which can be manipulated with `sif`:

```go
import (
	"github.com/go-sif/sif"
	"github.com/go-sif/sif/schema"
	"github.com/aws/aws-sdk-go/aws/session"
	s3Source "github.com/go-sif/sif-datasource-aws-s3"
)
// ...

parser := // ... any Sif parser

conf := &s3Source.DataSourceConf{
	Bucket:       "bucket.name",       // bucket name
	Prefix:       "/prefix/for/files", // S3 key prefix to filter which keys are accessed
	KeyBatchSize: 5,                   // The number of files assigned to a single worker at a
	                                   // time, to be downloaded concurrently with processing
	Session:      sess,
}

dataframe := s3Source.CreateDataFrame(conf, parser, schema)
```
