package local

import (
	"github.com/jamillosantos/filestorage"
	"os"
	"fmt"
	"io"
)

type object struct {
	id       string
	path     string
	metadata filestorage.Metadata
}

func newObject(id, fname string) (filestorage.Object, error) {
	stats, err := os.Stat(fname)
	if err != nil {
		return nil, err
	}
	if stats.IsDir() {
		return nil, fmt.Errorf("%s is a directory", id)
	}
	return &object{
		id:   id,
		path: fname,
	}, nil
}

func (o *object) ID() string {
	return o.id
}

func (o *object) Metadata() filestorage.Metadata {
	return o.metadata
}

func (o *object) Open() (io.ReadCloser, error) {
	return os.Open(o.path)
}

func (o *object) Remove() error {
	return os.Remove(o.path)
}
