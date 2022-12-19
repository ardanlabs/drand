package client_test

import (
	"context"
	"fmt"
	"testing"

	"github.com/drand/drand/client"
	"github.com/drand/drand/client/test/result/mock"
	"github.com/drand/drand/common/scheme"
	"github.com/drand/drand/log"
	"github.com/drand/drand/test/testlogger"
)

func mockClientWithVerifiableResults(l log.Logger, n int) (client.Client, []mock.Result, error) {
	sch := scheme.GetSchemeFromEnv()

	info, results := mock.VerifiableResults(n, sch)
	mc := client.MockClient{Results: results, StrictRounds: true, OptionalInfo: info}

	var c client.Client
	var err error

	c, err = client.Wrap(
		l,
		[]client.Client{client.MockClientWithInfo(info), &mc},
		client.WithChainInfo(info),
		client.WithVerifiedResult(&results[0]),
		client.WithFullChainVerification(),
	)

	if err != nil {
		return nil, nil, err
	}
	return c, results, nil
}

func TestVerify(t *testing.T) {
	VerifyFuncTest(t, 3, 1)
}

func TestVerifyWithOldVerifiedResult(t *testing.T) {
	VerifyFuncTest(t, 5, 4)
}

func VerifyFuncTest(t *testing.T, clients, upTo int) {
	l := testlogger.New(t)
	c, results, err := mockClientWithVerifiableResults(l, clients)
	if err != nil {
		t.Fatal(err)
	}
	res, err := c.Get(context.Background(), results[upTo].Round())
	if err != nil {
		t.Fatal(err)
	}
	if res.Round() != results[upTo].Round() {
		t.Fatal("expected to get result.", results[upTo].Round(), res.Round(), fmt.Sprintf("%v", c))
	}
}
