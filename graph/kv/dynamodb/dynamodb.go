package dynamodb

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/dynamodb"

	"github.com/cayleygraph/cayley/graph"
	"github.com/cayleygraph/cayley/graph/kv"
)

var (
	// AlwaysConsistent can be set to true to force DynamoDB to use consistent reads for all Get and Scan requests.
	// By default reads are eventually-consistent for RO transactions and consistent for RW transactions.
	AlwaysConsistent = false
)

const debug = false

const (
	keyField = "k"
	valField = "v"
)

func init() {
	kv.Register(Type, kv.Registration{
		InitFunc: func(addr string, opt graph.Options) (kv.BucketKV, error) {
			sess, err := getSess(opt)
			if err != nil {
				return nil, err
			}
			db, err := Init(sess, addr)
			if err != nil {
				return nil, err
			}
			return kv.FromFlat(db), nil
		},
		NewFunc: func(addr string, opt graph.Options) (kv.BucketKV, error) {
			sess, err := getSess(opt)
			if err != nil {
				return nil, err
			}
			db, err := New(sess, addr)
			if err != nil {
				return nil, err
			}
			return kv.FromFlat(db), nil
		},
		IsPersistent: true,
	})
}

func SetSession(opt graph.Options, sess *session.Session) {
	opt["session"] = sess
}

func newSession(access, secret, region string) (*session.Session, error) {
	cred := credentials.NewStaticCredentials(access, secret, "")
	return session.NewSession(&aws.Config{
		Region:      aws.String(region),
		Credentials: cred,
	})
}

var (
	awsAccessKey = os.Getenv("AWS_ACCESS_KEY")
	awsSecretKey = os.Getenv("AWS_SECRET_KEY")
	awsRegion    = os.Getenv("AWS_REGION")
)

func getSess(opt graph.Options) (*session.Session, error) {
	sess, _ := opt["session"].(*session.Session)
	if sess != nil {
		return sess, nil
	}
	access, _ := opt["access_key"].(string)
	secret, _ := opt["secret_key"].(string)
	region, _ := opt["region"].(string)
	if access == "" {
		access = awsAccessKey
	}
	if secret == "" {
		secret = awsSecretKey
	}
	if region == "" {
		region = awsRegion
	}
	if access == "" || secret == "" || region == "" {
		return nil, fmt.Errorf("no session or credentials specified")
	}
	return newSession(access, secret, region)
}

const Type = "dynamodb-kv"

var _ kv.FlatKV = (*DB)(nil)

func New(sess *session.Session, name string) (*DB, error) {
	ddb := dynamodb.New(sess)
	tbl := aws.String(name)
	_, err := ddb.DescribeTable(&dynamodb.DescribeTableInput{
		TableName: tbl,
	})
	if err != nil {
		return nil, err
	}
	return newDB(ddb, tbl), nil
}

func Init(sess *session.Session, name string) (*DB, error) {
	ddb := dynamodb.New(sess)
	tbl := aws.String(name)
	_, err := ddb.CreateTable(&dynamodb.CreateTableInput{
		TableName: tbl,
		AttributeDefinitions: []*dynamodb.AttributeDefinition{
			{
				AttributeName: aws.String(keyField),
				AttributeType: aws.String("B"),
			},
		},
		KeySchema: []*dynamodb.KeySchemaElement{
			{
				AttributeName: aws.String(keyField),
				KeyType:       aws.String("HASH"),
			},
		},
		ProvisionedThroughput: &dynamodb.ProvisionedThroughput{
			ReadCapacityUnits:  aws.Int64(10),
			WriteCapacityUnits: aws.Int64(3),
		},
	})
	if err != nil {
		return nil, err
	}
	if err = waitTable(context.TODO(), ddb, tbl); err != nil {
		return nil, err
	}
	return newDB(ddb, tbl), nil
}

func Wipe(sess *session.Session, name string) error {
	ddb := dynamodb.New(sess)
	tbl := aws.String(name)
	_, err := ddb.DeleteTable(&dynamodb.DeleteTableInput{
		TableName: tbl,
	})
	if err != nil {
		return err
	}
	return waitTableRM(context.TODO(), ddb, tbl)
}

func convError(err error) error {
	if e, ok := err.(awserr.Error); ok {
		switch e.Code() {
		case "ResourceNotFoundException":
			return kv.ErrNotFound
			//case "ConditionalCheckFailedException":
		}
	}
	return err
}

func waitTable(ctx context.Context, db *dynamodb.DynamoDB, name *string) error {
	for {
		resp, err := db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: name,
		})
		if err != nil {
			return err
		}
		switch *resp.Table.TableStatus {
		case "ACTIVE":
			return nil
		case "DELETING":
			return errors.New("table is being deleted")
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func waitTableRM(ctx context.Context, db *dynamodb.DynamoDB, name *string) error {
	for {
		_, err := db.DescribeTable(&dynamodb.DescribeTableInput{
			TableName: name,
		})
		err = convError(err)
		if err == kv.ErrNotFound {
			return nil
		} else if err != nil {
			return err
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(time.Second):
		}
	}
}

func newDB(db *dynamodb.DynamoDB, tbl *string) *DB {
	return &DB{db: db, tbl: tbl}
}

type DB struct {
	db  *dynamodb.DynamoDB
	tbl *string
}

func (db *DB) Type() string { return Type }
func (db *DB) Close() error {
	return nil
}
func (db *DB) Tx(update bool) (kv.FlatTx, error) {
	return &dynamoTx{db: db, ro: !update}, nil
}

type dynamoTx struct {
	db *DB
	ro bool

	keys   map[string]*dynamodb.WriteRequest
	buffer []*dynamodb.WriteRequest
}

const (
	maxWriteBatch = 25
	maxReadBatch  = 100
)

func (tx *dynamoTx) commit() error {
	if tx.ro || len(tx.buffer) == 0 {
		return nil
	}
	if debug {
		log.Printf("commit: %d", len(tx.buffer))
	}
	_, err := tx.db.db.BatchWriteItem(&dynamodb.BatchWriteItemInput{
		RequestItems: map[string][]*dynamodb.WriteRequest{
			*tx.db.tbl: tx.buffer,
		},
	})
	if err == nil {
		tx.keys = nil
		tx.buffer = tx.buffer[:0]
	}
	return err
}

func (tx *dynamoTx) Commit() error {
	return tx.commit()
}
func (tx *dynamoTx) Rollback() error {
	tx.keys = nil
	tx.buffer = nil
	return nil
}
func (tx *dynamoTx) consistent() *bool {
	if AlwaysConsistent {
		return aws.Bool(true)
	}
	return aws.Bool(!tx.ro)
}
func clone(p []byte) []byte {
	b := make([]byte, len(p))
	copy(b, p)
	return b
}
func (tx *dynamoTx) Get(keys [][]byte) ([][]byte, error) {
	vals := make([][]byte, len(keys))

	mi := make(map[string]int, len(keys))
	dkeys := make([]map[string]*dynamodb.AttributeValue, 0, len(keys))
	for i, k := range keys {
		if p := tx.keys[string(k)]; p != nil {
			if p.PutRequest != nil {
				vals[i] = clone(p.PutRequest.Item[valField].B)
			} else if p.DeleteRequest != nil {
				keys[i] = nil
			}
		} else {
			dkeys = append(dkeys, map[string]*dynamodb.AttributeValue{
				keyField: {B: k},
			})
			mi[string(k)] = i
		}
	}
	if len(dkeys) == 0 {
		return vals, nil
	}
	for len(dkeys) > 0 {
		batch := dkeys
		if len(batch) > maxReadBatch {
			batch = batch[:maxReadBatch]
		}
		if debug {
			if len(keys) <= maxReadBatch {
				log.Printf("batch get: %d, %q", len(keys), keys)
			} else {
				log.Printf("batch get: %d", len(batch))
			}
		}
		resp, err := tx.db.db.BatchGetItem(&dynamodb.BatchGetItemInput{
			RequestItems: map[string]*dynamodb.KeysAndAttributes{
				*tx.db.tbl: {
					ConsistentRead: tx.consistent(),
					Keys:           batch,
				},
			},
		})
		if err != nil {
			return nil, err
		}
		dkeys = dkeys[len(batch):]
		if len(resp.UnprocessedKeys) != 0 {
			if kvs := resp.UnprocessedKeys[*tx.db.tbl]; kvs != nil {
				dkeys = append(dkeys, kvs.Keys...)
			}
		}
		kvs := resp.Responses[*tx.db.tbl]
		for _, r := range kvs {
			if len(r) == 0 {
				continue
			}
			k := r[keyField]
			if k == nil {
				continue
			}
			ind := mi[string(k.B)]
			if p := r[valField]; p != nil {
				b := p.B
				if b == nil {
					b = []byte{}
				}
				vals[ind] = b
			}
		}
	}
	return vals, nil
}
func (tx *dynamoTx) getOne(k []byte) ([]byte, error) {
	if p := tx.keys[string(k)]; p != nil {
		if p.PutRequest != nil {
			return clone(p.PutRequest.Item[valField].B), nil
		} else if p.DeleteRequest != nil {
			return nil, kv.ErrNotFound
		}
	}
	if debug {
		log.Printf("get: %q", k)
	}
	resp, err := tx.db.db.GetItem(&dynamodb.GetItemInput{
		TableName:      tx.db.tbl,
		ConsistentRead: tx.consistent(),
		Key: map[string]*dynamodb.AttributeValue{
			keyField: {B: k},
		},
	})
	if err != nil { // FIXME: convert error
		return nil, err
	}
	v := resp.Item[valField]
	if v == nil {
		return nil, kv.ErrNotFound
	}
	return v.B, nil
}
func (tx *dynamoTx) write(k []byte, req *dynamodb.WriteRequest) error {
	if len(tx.buffer) >= maxWriteBatch {
		if err := tx.commit(); err != nil {
			return err
		}
	}
	if p := tx.keys[string(k)]; p != nil {
		*p = *req
	} else {
		p = req
		if tx.keys == nil {
			tx.keys = make(map[string]*dynamodb.WriteRequest)
		}
		tx.keys[string(k)] = p
		tx.buffer = append(tx.buffer, p)
	}
	return nil
}
func (tx *dynamoTx) Put(k, v []byte) error {
	if tx.ro {
		return fmt.Errorf("put on ro tx")
	}
	return tx.write(k, &dynamodb.WriteRequest{
		PutRequest: &dynamodb.PutRequest{
			Item: map[string]*dynamodb.AttributeValue{
				keyField: {B: k},
				valField: {B: v},
			},
		},
	})
}
func (tx *dynamoTx) Del(k []byte) error {
	if tx.ro {
		return fmt.Errorf("del on ro tx")
	}
	return tx.write(k, &dynamodb.WriteRequest{
		DeleteRequest: &dynamodb.DeleteRequest{
			Key: map[string]*dynamodb.AttributeValue{
				keyField: {B: k},
			},
		},
	})
}
func (tx *dynamoTx) Scan(pref []byte) kv.KVIterator {
	req := &dynamodb.ScanInput{
		TableName:      tx.db.tbl,
		ConsistentRead: tx.consistent(),
	}
	if len(pref) != 0 {
		req.ScanFilter = map[string]*dynamodb.Condition{
			keyField: {
				ComparisonOperator: aws.String(dynamodb.ComparisonOperatorBeginsWith),
				AttributeValueList: []*dynamodb.AttributeValue{{B: pref}},
			},
		}
	}
	return &Iterator{db: tx.db.db, req: req}
}

type Iterator struct {
	db  *dynamodb.DynamoDB
	req *dynamodb.ScanInput

	err error
	buf []map[string]*dynamodb.AttributeValue
}

func (it *Iterator) Next(ctx context.Context) bool {
	if it.buf == nil {
		resp, err := it.db.Scan(it.req)
		if err != nil {
			it.err = err
			return false
		}
		if debug {
			log.Println("scan page:", len(resp.Items))
		}
		it.req.ExclusiveStartKey = resp.LastEvaluatedKey
		it.buf = resp.Items
		return len(it.buf) > 0
	}
	if len(it.buf) == 0 {
		return false
	}
	it.buf = it.buf[1:]
	return len(it.buf) != 0
}
func (it *Iterator) Err() error { return it.err }
func (it *Iterator) Close() error {
	it.req = nil
	it.buf = nil
	return it.err
}
func (it *Iterator) bytesProp(name string) []byte {
	if len(it.buf) == 0 {
		return nil
	}
	v := it.buf[0][name]
	if v == nil {
		return nil
	}
	return v.B
}
func (it *Iterator) Key() []byte {
	return it.bytesProp(keyField)
}
func (it *Iterator) Val() []byte {
	return it.bytesProp(valField)
}
