package cluster

import (
	"testing"
	"github.com/go-kit/kit/log"
	"github.com/improbable-eng/thanos/pkg/testutil"
	"io/ioutil"
	"os"
	"time"
)

func TestFilePeerList_WaitsForContent(t *testing.T) {
	file, err := ioutil.TempFile("", "fileLoading")
	testutil.Ok(t, err)
	defer os.Remove(file.Name())
	ioutil.WriteFile(file.Name(), []byte(""), 0600)

	go func() {
		time.Sleep(1*time.Second)
		ioutil.WriteFile(file.Name(), []byte("2.3.4.5:34"), 0600)
	}()

	subject := FilePeerList(file.Name())(log.NewNopLogger())

	addresses, err := subject.ResolvePeers("not-used", true)
	testutil.Ok(t, err)
	testutil.Assert(t, len(addresses) == 1, "Expected to return addresses")
	testutil.Assert(t, contains("2.3.4.5:34", addresses), "Expected to contain address")
}

func TestFilePeerList_EmptyFileDoesNotWait(t *testing.T) {
	file, err := ioutil.TempFile("", "fileLoading")
	testutil.Ok(t, err)
	defer os.Remove(file.Name())
	ioutil.WriteFile(file.Name(), []byte(""), 0600)

	subject := FilePeerList(file.Name())(log.NewNopLogger())

	addresses, err := subject.ResolvePeers("not-used", false)
	testutil.Ok(t, err)
	testutil.Assert(t, len(addresses) == 0, "Expected to return no addresses")
}

func TestFilePeerList_MyAddressRemoved(t *testing.T) {
	file, err := ioutil.TempFile("", "fileLoading")
	testutil.Ok(t, err)
	defer os.Remove(file.Name())
	ioutil.WriteFile(file.Name(), []byte(`2.3.4.5:34
1.2.3.4:23
9.8.7.6:67
`), 0600)

	subject := FilePeerList(file.Name())(log.NewNopLogger())

	addresses, err := subject.ResolvePeers("1.2.3.4:23", false)
	testutil.Ok(t, err)
	testutil.Assert(t, len(addresses) == 2, "Expected to return no addresses")
	testutil.Assert(t, contains("2.3.4.5:34", addresses), "Expected to contain first address")
	testutil.Assert(t, contains("9.8.7.6:67", addresses), "Expected to contain second address")
}

func contains(needle string, haystack []string) bool {
	for _, item := range haystack {
		if item == needle {
			return true
		}
	}
	return false
}
