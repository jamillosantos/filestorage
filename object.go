package filestorage

type Object interface {
	ID() string
	Remove() error
}
