package dynamodb

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
	"github.com/cayleygraph/cayley/graph/kv/kvtest"

	"github.com/aws/aws-sdk-go/aws/session"
)

func getSession(t testing.TB) *session.Session {
	if awsAccessKey == "" || awsSecretKey == "" || awsRegion == "" {
		t.SkipNow()
	}
	sess, err := newSession(awsAccessKey, awsSecretKey, awsRegion)
	if err != nil {
		t.Fatal(err)
	}
	return sess
}

func newDynamo(t testing.TB) (kv.BucketKV, graph.Options, func()) {
	sess := getSession(t)
	name := fmt.Sprintf("cayley-test-%x", rand.Int())
	db, err := Init(sess, name)
	if err != nil {
		t.Fatal(err)
	}
	return kv.FromFlat(db), nil, func() {
		if err = Wipe(sess, name); err != nil {
			t.Logf("failed to remove table %q: %v", name, err)
		}
	}
}

func TestDynamodbKV(t *testing.T) {
	kvtest.TestAll(t, newDynamo, nil)
}
