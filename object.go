package filestorage

import "io"

type Object interface {
	ID() string
	Metadata() Metadata
	Open() (io.ReadCloser, error)
	Remove() error
	URL() string
}
