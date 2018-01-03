package filestorage

type Storage interface {
	Bucket(id string) (Bucket, error)
	CreateBucket(id string) (Bucket, error)
	CreateOrGetBucket(id string) (Bucket, error)
	RemoveBucket(id string) error
}
