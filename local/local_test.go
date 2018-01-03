package local_test

import (
	"testing"
	"github.com/jamillosantos/macchiato"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

func TestLocal(t *testing.T) {
	RegisterFailHandler(Fail)
	macchiato.RunSpecs(t, "FileStorage Local Test Suite")
}
