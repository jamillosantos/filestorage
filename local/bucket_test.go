package local_test

import (
	"os"
	"path"
	"bytes"
	"io/ioutil"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/jamillosantos/filestorage/local"
)

var _ = Describe("Local", func() {
	Describe("Bucket", func() {
		commonCfg := local.LocalStorageConfiguration{
			Directory:         "",
			NewBucketFileMode: 0755,
			NewObjectFileMode: 0755,
		}

		BeforeEach(func() {
			dir, err := ioutil.TempDir(os.TempDir(), "storage")
			Expect(err).To(BeNil())
			commonCfg.Directory = dir
		})

		It("should return the correct bucket id", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucket1"

			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())
			Expect(bucket.ID()).To(Equal(bucketID))
		})

		It("should fail when getting a non existent file", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			const bucketID = "bucket1"
			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			const objectID = "object1"
			object, err := bucket.Object(objectID)
			Expect(err).NotTo(BeNil())
			Expect(object).To(BeNil())
		})

		It("should fail to put a file into the bucket (with metadata)", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			const bucketID = "bucket1"
			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			const objectID = "object1"
			reader := bytes.NewBufferString("this is a file content")
			object, err := bucket.PutObject(objectID, reader, int64(reader.Len()), map[string]interface{}{
				"data1": "value1",
				"data2": 2,
			})
			Expect(err).NotTo(BeNil())
			Expect(object).To(BeNil())
			Expect(err.Error()).To(Equal("metadata not supported"))
		})

		It("should put a file into the bucket (without metadata)", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			const bucketID = "bucket1"
			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			const objectID = "object1"

			fileContent := "this is a file content"
			reader := bytes.NewBufferString(fileContent)
			readerLen := int64(reader.Len())
			object, err := bucket.PutObject(objectID, reader, readerLen, nil)
			Expect(err).To(BeNil())
			Expect(object).NotTo(BeNil())
			Expect(object.ID()).To(Equal(objectID))

			stats, err := os.Stat(path.Join(commonCfg.Directory, bucketID, objectID))
			Expect(err).To(BeNil())
			Expect(stats.IsDir()).To(BeFalse())
			Expect(stats.Size()).To(Equal(readerLen))
			fc, err := ioutil.ReadFile(path.Join(commonCfg.Directory, bucketID, objectID))
			Expect(err).To(BeNil())
			Expect(bytes.Equal([]byte(fileContent), fc)).To(BeTrue())
		})

		It("should put a file into the bucket with zero size (auto size) (without metadata)", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			const bucketID = "bucket1"
			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			const objectID = "object1"

			fileContent := "this is a file content"
			reader := bytes.NewBufferString(fileContent)
			readerLen := int64(0)
			object, err := bucket.PutObject(objectID, reader, readerLen, nil)
			Expect(err).To(BeNil())
			Expect(object).NotTo(BeNil())
			Expect(object.ID()).To(Equal(objectID))

			stats, err := os.Stat(path.Join(commonCfg.Directory, bucketID, objectID))
			Expect(err).To(BeNil())
			Expect(stats.IsDir()).To(BeFalse())
			Expect(stats.Size()).To(Equal(int64(len(fileContent))))
			fc, err := ioutil.ReadFile(path.Join(commonCfg.Directory, bucketID, objectID))
			Expect(err).To(BeNil())
			Expect(bytes.Equal([]byte(fileContent), fc)).To(BeTrue())
		})

		It("should put a file into the bucket with less data than the available on the reader (without metadata)", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			const bucketID = "bucket1"
			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			const objectID = "object1"

			fileContent := "this is a file content"
			reader := bytes.NewBufferString(fileContent)
			readerLen := int64(5)
			object, err := bucket.PutObject(objectID, reader, readerLen, nil)
			Expect(err).To(BeNil())
			Expect(object).NotTo(BeNil())
			Expect(object.ID()).To(Equal(objectID))

			stats, err := os.Stat(path.Join(commonCfg.Directory, bucketID, objectID))
			Expect(err).To(BeNil())
			Expect(stats.IsDir()).To(BeFalse())
			Expect(stats.Size()).To(Equal(readerLen))
			fc, err := ioutil.ReadFile(path.Join(commonCfg.Directory, bucketID, objectID))
			Expect(err).To(BeNil())
			Expect(bytes.Equal([]byte(fileContent[:readerLen]), fc)).To(BeTrue())
		})
	})
})
