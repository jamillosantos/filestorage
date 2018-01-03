package local

import (
	"github.com/jamillosantos/filestorage"
	"os"
	"fmt"
	"path"
	"sync"
)

type LocalStorageConfiguration struct {
	Directory         string
	NewBucketFileMode os.FileMode
	NewObjectFileMode os.FileMode
}

type storage struct {
	m    sync.Mutex
	conf LocalStorageConfiguration
}

func NewStorage(configuration LocalStorageConfiguration) (filestorage.Storage, error) {
	f, err := os.Stat(configuration.Directory)
	if err != nil {
		return nil, err
	}
	if !f.IsDir() {
		return nil, fmt.Errorf("%s is not a directory", configuration.Directory)
	}
	return &storage{
		conf: configuration,
	}, nil
}

func (s *storage) bucketPath(id string) string {
	return path.Join(s.conf.Directory, id)
}

func (s *storage) Bucket(id string) (filestorage.Bucket, error) {
	s.m.Lock()
	defer s.m.Unlock()
	return newBucket(s, id)
}

func (s *storage) createBucketWithoutLock(id string) (filestorage.Bucket, error) {
	err := os.Mkdir(s.bucketPath(id), s.conf.NewBucketFileMode)
	if err != nil {
		return nil, err
	}
	return newBucket(s, id)
}

func (s *storage) CreateBucket(id string) (filestorage.Bucket, error) {
	s.m.Lock()
	defer s.m.Unlock()
	return s.createBucketWithoutLock(id)
}

func (s *storage) CreateOrGetBucket(id string) (filestorage.Bucket, error) {
	s.m.Lock()
	defer s.m.Unlock()

	_, err := os.Stat(s.bucketPath(id))
	if err != nil {
		if os.IsNotExist(err) {
			return s.createBucketWithoutLock(id)
		}
		return nil, err
	}
	return newBucket(s, id)
}

func (s *storage) RemoveBucket(id string) error {
	return os.RemoveAll(path.Join(s.conf.Directory, id))
}
