package s3

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/go-sif/sif"
)

func newPartitionMap(source *DataSource) (sif.PartitionMap, error) {
	result := &PartitionMap{
		currentObject: 0,
		currentList:   nil,
		source:        source,
		s3Client:      s3.New(source.conf.Session),
	}
	err := result.fetchNextPage()
	if err != nil {
		return nil, err
	}
	return result, nil
}

// PartitionMap is an iterator producing a sequence of PartitionLoaders
type PartitionMap struct {
	currentObject int
	currentList   *s3.ListObjectsV2Output
	s3Client      *s3.S3
	source        *DataSource
}

func (pm *PartitionMap) fetchNextPage() error {
	req := s3.ListObjectsV2Input{
		Bucket:       aws.String(pm.source.conf.Bucket),
		Prefix:       aws.String(pm.source.conf.Prefix),
		RequestPayer: aws.String(pm.source.conf.RequestPayer),
		MaxKeys:      aws.Int64(pm.source.conf.KeyBatchSize),
	}
	if pm.currentList != nil {
		// we're done
		if pm.currentList.NextContinuationToken == nil {
			pm.currentList = nil
			return nil
		}
		req.ContinuationToken = pm.currentList.NextContinuationToken
	}
	out, err := pm.s3Client.ListObjectsV2(&req)
	if err != nil {
		return err
	}
	pm.currentList = out
	return nil
}

// HasNext returns true iff there is another PartitionLoader remaining
func (pm *PartitionMap) HasNext() bool {
	return pm.currentList != nil
}

// Next returns the next PartitionLoader for a file
func (pm *PartitionMap) Next() sif.PartitionLoader {
	keys := make([]string, len(pm.currentList.Contents))
	for i, v := range pm.currentList.Contents {
		keys[i] = *v.Key
	}
	defer pm.fetchNextPage()
	return &PartitionLoader{
		keys:   keys,
		source: pm.source,
	}
}
