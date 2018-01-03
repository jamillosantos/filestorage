package filestorage

import "io"

type Bucket interface {
	ID() string
	Object(id string, metadata Metadata) (Object, error)
	PutObject(id string, reader io.Reader) error
}
