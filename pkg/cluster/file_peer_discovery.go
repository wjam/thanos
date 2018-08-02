package cluster

import (
	"github.com/go-kit/kit/log"
	"io/ioutil"
	"github.com/improbable-eng/thanos/pkg/runutil"
	"context"
	"time"
	"strings"
	"github.com/pkg/errors"
)

func FilePeerList(fileName string) func(log.Logger) PeerDiscovery {
	return func(logger log.Logger) PeerDiscovery {
		return &filePeerDiscovery{
			fileName: fileName,
		}
	}
}

type filePeerDiscovery struct {
	fileName string
}

func (f *filePeerDiscovery) ResolvePeers(myAddress string, waitIfEmpty bool) ([]string, error) {
	ctx := context.Background()
	retryCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var addresses []string
	err := runutil.Retry(2*time.Second, retryCtx.Done(), func() error {
		content, err := ioutil.ReadFile(f.fileName)
		if err != nil {
			return err
		}

		allAddresses := strings.Split(string(content), "\n")
		addresses = removeMyAddress(allAddresses, myAddress)
		if len(addresses) == 0 {
			if !waitIfEmpty {
				return nil
			}
			return errors.New("empty file. Retrying")
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return addresses, nil
}

func removeMyAddress(allAddresses []string, myAddress string) []string {
	var addresses []string
	for _, address := range allAddresses {
		if address != myAddress && address != "" {
			addresses = append(addresses, address)
		}
	}
	return addresses
}
