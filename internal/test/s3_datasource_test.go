package test

import (
	"bufio"
	"log"
	"os"
	"path/filepath"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/endpoints"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
	"github.com/go-sif/sif"
	sifs3 "github.com/go-sif/sif-datasource-aws-s3"
	"github.com/go-sif/sif/datasource/parser/jsonl"
	"github.com/go-sif/sif/errors"
	"github.com/go-sif/sif/schema"
	"github.com/stretchr/testify/require"
)

func TestS3Datasource(t *testing.T) {
	schema := schema.CreateSchema()
	schema.CreateColumn("coords.x", &sif.Float64ColumnType{})
	schema.CreateColumn("coords.z", &sif.Float64ColumnType{})
	schema.CreateColumn("date", &sif.TimeColumnType{Format: "2006-01-02 15:04:05"})

	sess := session.Must(session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials("foo", "bar", ""),
		S3ForcePathStyle: aws.Bool(true),
		Region:           aws.String(endpoints.UsEast1RegionID),
		Endpoint:         aws.String("http://localhost:4566"),
	}))

	svc := s3.New(sess)

	// create a test bucket
	_, err := svc.CreateBucket(&s3.CreateBucketInput{
		Bucket: aws.String("testbucket"),
	})
	require.Nil(t, err)

	defer svc.DeleteBucket(&s3.DeleteBucketInput{
		Bucket: aws.String("testbucket"),
	})

	// upload test data to the bucket
	expectedRows := 0
	uploader := s3manager.NewUploaderWithClient(svc)
	err = filepath.Walk("../../testdata/", func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return nil
		}
		name := filepath.Join("../../testdata/", info.Name())
		f, err := os.Open(name)
		if err != nil {
			return err
		}
		defer f.Close()

		keyName := filepath.Join("files", info.Name())
		log.Printf("Uploading test file %s", keyName)
		upParams := &s3manager.UploadInput{
			Bucket: aws.String("testbucket"),
			Key:    aws.String(keyName),
			Body:   f,
		}
		_, err = uploader.Upload(upParams)
		if err != nil {
			return err
		}

		// add to expected rows count
		fileScanner := bufio.NewScanner(f)
		for fileScanner.Scan() {
			expectedRows++
		}
		return nil
	})
	require.Nil(t, err)

	req := &s3.ListObjectsV2Input{
		Bucket:  aws.String("testbucket"),
		Prefix:  aws.String("files"),
		MaxKeys: aws.Int64(6),
	}
	out, err := svc.ListObjectsV2(req)
	require.Equal(t, int64(6), *out.KeyCount)

	source := sifs3.CreateDataFrame(&sifs3.DataSourceConf{
		Bucket:       "testbucket",
		Prefix:       "files",
		KeyBatchSize: 2,
		Session:      sess,
	}, jsonl.CreateParser(&jsonl.ParserConf{
		PartitionSize: 128,
	}), schema)

	pmap, err := source.GetDataSource().Analyze()
	totalRows := 0
	require.Nil(t, err)
	for pmap.HasNext() {
		pl := pmap.Next()
		pi, err := pl.Load(source.GetParser(), schema)
		require.Nil(t, err)
		for pi.HasNextPartition() {
			part, _, err := pi.NextPartition()
			if _, ok := err.(errors.NoMorePartitionsError); ok {
				continue
			}
			require.Nil(t, err)
			totalRows += part.GetNumRows()
		}
	}
	require.Equal(t, expectedRows, totalRows)
}
