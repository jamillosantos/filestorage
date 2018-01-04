package local

import (
	"io"
	"path"

	"github.com/jamillosantos/filestorage"
	"os"
	"fmt"
	"github.com/pkg/errors"
)

type bucket struct {
	id               string
	path             string
	createObjectMode os.FileMode
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
		id:               id,
		path:             d,
		createObjectMode: storage.conf.NewObjectFileMode,
	}, nil
}

func (b *bucket) ID() string {
	return b.id
}

func (b *bucket) Object(id string) (filestorage.Object, error) {
	fname := path.Join(b.path, id)
	stats, err := os.Stat(fname)
	if err != nil {
		return nil, err
	}
	if stats.IsDir() {
		return nil, errors.New("the object happens to be a directory")
	}
	obj, err := newObject(id, fname)
	return obj, err
}

func (b *bucket) PutObject(id string, reader io.Reader, size int64, metadata filestorage.Metadata) (filestorage.Object, error) {
	if metadata != nil {
		return nil, errors.New("metadata not supported")
	}
	fname := path.Join(b.path, id)
	_, err := os.Stat(fname)
	if err != nil {
		if !os.IsNotExist(err) {
			return nil, err
		}
	}
	writer, err := os.OpenFile(fname, os.O_CREATE|os.O_WRONLY, b.createObjectMode)
	if err != nil {
		return nil, err
	}
	defer writer.Close()
	if size == 0 {
		_, err = io.Copy(writer, reader)
	} else {
		_, err = io.CopyN(writer, reader, size)
	}
	if err != nil {
		defer os.Remove(fname) // Sorry if it fails, we hope it not. Shame on me :/.
		return nil, err
	}
	return newObject(id, fname)
}
