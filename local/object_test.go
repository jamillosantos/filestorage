package local_test

import (
	"os"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/jamillosantos/filestorage/local"
	"io/ioutil"
	"bytes"
	"path"
	"fmt"
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

		It("should return the correct object ID when creating an object", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucket1"

			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())

			objectID := "object1"
			reader := bytes.NewBufferString("this is the file content")
			obj, err := bucket.PutObject(objectID, reader, int64(reader.Len()), nil)
			Expect(err).To(BeNil())
			Expect(obj.ID()).To(Equal(objectID))
		})

		It("should return the correct object URL when creating an object", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucket1"

			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())

			objectID := "object1"
			reader := bytes.NewBufferString("this is the file content")
			obj, err := bucket.PutObject(objectID, reader, int64(reader.Len()), nil)
			Expect(err).To(BeNil())
			Expect(obj.URL()).To(Equal(fmt.Sprintf("%s/%s", bucketID, objectID)))
		})

		It("should return the correct object ID when getting an object", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucket1"

			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())

			objectID := "object1"
			reader := bytes.NewBufferString("this is the file content")
			_, err = bucket.PutObject(objectID, reader, 0, nil)
			Expect(err).To(BeNil())

			obj, err := bucket.Object(objectID)
			Expect(err).To(BeNil())
			Expect(obj).NotTo(BeNil())
			Expect(obj.ID()).To(Equal(objectID))
		})

		It("should return the correct object URL when getting an object", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucket1"

			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())

			objectID := "object1"
			reader := bytes.NewBufferString("this is the file content")
			_, err = bucket.PutObject(objectID, reader, 0, nil)
			Expect(err).To(BeNil())

			obj, err := bucket.Object(objectID)
			Expect(err).To(BeNil())
			Expect(obj).NotTo(BeNil())
			Expect(obj.URL()).To(Equal(fmt.Sprintf("%s/%s", bucketID, objectID)))
		})

		It("should remove an object", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucket1"
			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())

			objectID := "object1"
			reader := bytes.NewBufferString("this is the file content")
			obj, err := bucket.PutObject(objectID, reader, 0, nil)
			Expect(err).To(BeNil())
			Expect(obj.Remove()).To(BeNil())

			_, err = os.Stat(path.Join(commonCfg.Directory, bucketID, objectID))
			Expect(err).NotTo(BeNil())
			Expect(os.IsNotExist(err)).To(BeTrue())
		})

		It("should open an object", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucket1"
			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())

			objectID := "object1"
			fileContent := "this is the file content"
			reader := bytes.NewBufferString(fileContent)
			obj, err := bucket.PutObject(objectID, reader, 0, nil)
			Expect(err).To(BeNil())

			fileContentReader, err := obj.Open()
			Expect(err).To(BeNil())
			Expect(fileContentReader).NotTo(BeNil())
			defer fileContentReader.Close()
			content, err := ioutil.ReadAll(fileContentReader)
			Expect(err).To(BeNil())
			Expect(string(content)).To(Equal(fileContent))
		})
	})
})
