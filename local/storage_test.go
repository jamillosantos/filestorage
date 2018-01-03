package local_test

import (
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"

	"github.com/jamillosantos/filestorage/local"
	"path"
	"os"
	"runtime"
)

var _ = Describe("Local", func() {
	Describe("Storage", func() {
		_, file, _, _ := runtime.Caller(0)
		testDirectory := path.Join(path.Dir(path.Dir(file)), "tests")

		commonCfg := local.LocalStorageConfiguration{
			Directory:         testDirectory,
			NewBucketFileMode: 0755,
			NewObjectFileMode: 0755,
		}

		BeforeEach(func() {
			Expect(os.RemoveAll(testDirectory)).To(BeNil())
			Expect(os.MkdirAll(testDirectory, 0755)).To(BeNil())
		})

		It("should fail creating a local storage from a non existent directory", func() {
			storage, err := local.NewStorage(local.LocalStorageConfiguration{
				Directory: "non existent directory",
			})
			Expect(err).NotTo(BeNil())
			Expect(storage).To(BeNil())
		})

		It("should return a new instance of a local storage", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())
		})

		It("should create a bucket", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucketID1"

			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())
			Expect(bucket.ID()).To(Equal(bucketID))

			stats, err := os.Stat(path.Join(testDirectory, bucketID))
			Expect(err).To(BeNil())
			Expect(stats.IsDir()).To(BeTrue())
		})

		It("should remove a bucket", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucketID2"

			bucket, err := storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())
			Expect(bucket.ID()).To(Equal(bucketID))

			err = storage.RemoveBucket(bucketID)
			Expect(err).To(BeNil())

			_, err = os.Stat(path.Join(testDirectory, bucketID))
			Expect(err).NotTo(BeNil())
			Expect(os.IsNotExist(err)).To(BeTrue())
		})

		It("should create or return a bucket (it will create)", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucketID3"

			bucket, err := storage.CreateOrGetBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())
			Expect(bucket.ID()).To(Equal(bucketID))

			stats, err := os.Stat(path.Join(testDirectory, bucketID))
			Expect(err).To(BeNil())
			Expect(stats.IsDir()).To(BeTrue())
		})

		It("should create or return a bucket (it will return)", func() {
			storage, err := local.NewStorage(commonCfg)
			Expect(err).To(BeNil())
			Expect(storage).NotTo(BeNil())

			const bucketID = "bucketID4"

			_, err = storage.CreateBucket(bucketID)
			Expect(err).To(BeNil())

			bucket, err := storage.CreateOrGetBucket(bucketID)
			Expect(err).To(BeNil())
			Expect(bucket).NotTo(BeNil())
			Expect(bucket.ID()).To(Equal(bucketID))

			stats, err := os.Stat(path.Join(testDirectory, bucketID))
			Expect(err).To(BeNil())
			Expect(stats.IsDir()).To(BeTrue())
		})
	})
})
