package filestorage

import "io"

type Bucket interface {
	ID() string
	Object(id string) (Object, error)
	PutObject(id string, reader io.Reader, size int64, metadata Metadata) (Object, error)
}
