package local

import (
	"io"
	"path"

	"github.com/jamillosantos/filestorage"
	"os"
	"fmt"
)

type bucket struct {
	id   string
	path string
}

func newBucket(storage *storage, id string) (filestorage.Bucket, error) {
	d := path.Join(storage.conf.Directory, id)
	stats, err := os.Stat(d)
	if err != nil {
		return nil, err
	}
	if !stats.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", d)
	}
	return &bucket{
		id:   id,
		path: d,
	}, nil
}

func (b *bucket) ID() string {
	return b.id
}

func (b *bucket) Object(id string, metadata filestorage.Metadata) (filestorage.Object, error) {
	panic("implement me")
}

func (b *bucket) PutObject(id string, reader io.Reader) error {
	panic("implement me")
}
