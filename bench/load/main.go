// Command load drives an AWS emulator with the AWS SDK for Go and reports the
// throughput and latency of common operations at several concurrencies.
//
//	go run ./bench/load -endpoint http://localhost:4566 -label stratus -out load.csv
//
// With -container it also samples the container's memory and reports the peak
// of each measurement.
package main

import (
	"bytes"
	"cmp"
	"context"
	"crypto/rand"
	"encoding/csv"
	"errors"
	"flag"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	ddbt "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/kinesis"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3t "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

// op is one operation; i is unique per call, for keys.
type op struct {
	name string
	call func(ctx context.Context, i int64) error
}

type clients struct {
	ddb   *dynamodb.Client
	s3    *s3.Client
	sqs   *sqs.Client
	kin   *kinesis.Client
	queue string
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "load:", err)
		os.Exit(1)
	}
}

func run() error {
	endpoint := flag.String("endpoint", "http://localhost:4566", "emulator endpoint")
	label := flag.String("label", "", "name of the emulator in the results")
	out := flag.String("out", "", "CSV file to append the results to")
	duration := flag.Duration("duration", 8*time.Second, "length of each measurement")
	concList := flag.String("conc", "1,32,128", "comma-separated numbers of concurrent clients")
	only := flag.String("ops", "", "run only the operations whose name starts with this")
	container := flag.String("container", "", "docker container whose memory to sample, if any")
	flag.Parse()
	if *label == "" || *out == "" {
		return errors.New("-label and -out are required")
	}
	var conc []int
	for _, s := range strings.Split(*concList, ",") {
		n, err := strconv.Atoi(strings.TrimSpace(s))
		if err != nil || n < 1 {
			return fmt.Errorf("bad -conc %q", *concList)
		}
		conc = append(conc, n)
	}

	ctx := context.Background()
	c := newClients(*endpoint)
	if err := setup(ctx, c); err != nil {
		return fmt.Errorf("creating the benchmark's resources: %w", err)
	}
	f, err := os.OpenFile(*out, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0o644)
	if err != nil {
		return err
	}
	defer f.Close()
	w := csv.NewWriter(f)
	if info, _ := f.Stat(); info.Size() == 0 {
		_ = w.Write([]string{"emulator", "op", "conc", "ops_s", "p50_ms", "p99_ms", "errors", "peak_mem_mib"})
	}
	for _, o := range operations(c) {
		if !strings.HasPrefix(o.name, *only) {
			continue
		}
		for _, n := range conc {
			r := measure(ctx, o, n, *duration, *container)
			row := []string{*label, o.name, strconv.Itoa(n), fmt.Sprintf("%.0f", r.opsPerSec),
				fmt.Sprintf("%.2f", r.p50), fmt.Sprintf("%.2f", r.p99), strconv.Itoa(r.errors), fmt.Sprintf("%.0f", r.peakMiB)}
			_ = w.Write(row)
			w.Flush()
			fmt.Println(strings.Join(row, " "))
		}
	}
	return w.Error()
}

func newClients(endpoint string) *clients {
	hc := &http.Client{Transport: &http.Transport{MaxIdleConnsPerHost: 512, MaxConnsPerHost: 512}}
	cfg := aws.Config{
		Region: "eu-central-1", HTTPClient: hc, RetryMaxAttempts: 1,
		Credentials: aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
			return aws.Credentials{AccessKeyID: "test", SecretAccessKey: "test"}, nil
		}),
	}
	return &clients{
		ddb: dynamodb.NewFromConfig(cfg, func(o *dynamodb.Options) { o.BaseEndpoint = &endpoint }),
		s3:  s3.NewFromConfig(cfg, func(o *s3.Options) { o.BaseEndpoint = &endpoint; o.UsePathStyle = true }),
		sqs: sqs.NewFromConfig(cfg, func(o *sqs.Options) { o.BaseEndpoint = &endpoint }),
		kin: kinesis.NewFromConfig(cfg, func(o *kinesis.Options) { o.BaseEndpoint = &endpoint }),
	}
}

func operations(c *clients) []op {
	small, large := randomBytes(4<<10), randomBytes(256<<10)
	item := func(pk string) map[string]ddbt.AttributeValue {
		return map[string]ddbt.AttributeValue{
			"pk": &ddbt.AttributeValueMemberS{Value: pk}, "sk": &ddbt.AttributeValueMemberS{Value: "a"},
			"v": &ddbt.AttributeValueMemberS{Value: string(small[:200])},
		}
	}
	put := func(body []byte) func(context.Context, int64) error {
		return func(ctx context.Context, i int64) error {
			_, err := c.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("bench"), Key: aws.String("k/" + strconv.FormatInt(i, 36)), Body: bytes.NewReader(body)})
			return err
		}
	}
	get := func(key string) func(context.Context, int64) error {
		return func(ctx context.Context, _ int64) error {
			o, err := c.s3.GetObject(ctx, &s3.GetObjectInput{Bucket: aws.String("bench"), Key: aws.String(key)})
			if err != nil {
				return err
			}
			_, err = io.Copy(io.Discard, o.Body)
			_ = o.Body.Close()
			return err
		}
	}
	return []op{
		{"dynamodb.PutItem", func(ctx context.Context, i int64) error {
			_, err := c.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("bench"), Item: item(strconv.FormatInt(i, 36))})
			return err
		}},
		{"dynamodb.GetItem", func(ctx context.Context, i int64) error {
			_, err := c.ddb.GetItem(ctx, &dynamodb.GetItemInput{TableName: aws.String("bench"), Key: map[string]ddbt.AttributeValue{
				"pk": &ddbt.AttributeValueMemberS{Value: "hot"}, "sk": &ddbt.AttributeValueMemberS{Value: fmt.Sprintf("%04d", i%200)}}})
			return err
		}},
		{"dynamodb.Query", func(ctx context.Context, _ int64) error {
			_, err := c.ddb.Query(ctx, &dynamodb.QueryInput{TableName: aws.String("bench"), KeyConditionExpression: aws.String("pk = :p"),
				ExpressionAttributeValues: map[string]ddbt.AttributeValue{":p": &ddbt.AttributeValueMemberS{Value: "hot"}}, Limit: aws.Int32(50)})
			return err
		}},
		{"s3.PutObject 4KiB", put(small)},
		{"s3.PutObject 256KiB", put(large)},
		{"s3.GetObject 4KiB", get("small")},
		{"s3.GetObject 256KiB", get("large")},
		{"sqs.SendMessage", func(ctx context.Context, _ int64) error {
			// SQS accepts only XML characters in a body: random bytes would be refused.
			_, err := c.sqs.SendMessage(ctx, &sqs.SendMessageInput{QueueUrl: &c.queue, MessageBody: aws.String(messageBody)})
			return err
		}},
		{"sqs.ReceiveDelete", func(ctx context.Context, _ int64) error {
			out, err := c.sqs.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{QueueUrl: &c.queue, MaxNumberOfMessages: 10})
			if err != nil {
				return err
			}
			for _, m := range out.Messages {
				if _, err := c.sqs.DeleteMessage(ctx, &sqs.DeleteMessageInput{QueueUrl: &c.queue, ReceiptHandle: m.ReceiptHandle}); err != nil {
					return err
				}
			}
			return nil
		}},
		{"kinesis.PutRecord", func(ctx context.Context, i int64) error {
			_, err := c.kin.PutRecord(ctx, &kinesis.PutRecordInput{StreamName: aws.String("bench"), Data: small[:1024], PartitionKey: aws.String(strconv.FormatInt(i, 36))})
			return err
		}},
	}
}

type result struct {
	opsPerSec, p50, p99, peakMiB float64
	errors                       int
}

func measure(ctx context.Context, o op, conc int, d time.Duration, container string) result {
	stopSampling := sampleMemory(container)
	var (
		mu   sync.Mutex
		lat  []time.Duration
		errs int
		// firstErr is printed to stderr, so that an error count has a cause.
		firstErr error
		next     atomic.Int64
		wg       sync.WaitGroup
	)
	began := time.Now()
	stop := began.Add(d)
	for range conc {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var mine []time.Duration
			failed := 0
			var first error
			for time.Now().Before(stop) {
				t := time.Now()
				if err := o.call(ctx, next.Add(1)); err != nil {
					failed++
					first = cmp.Or(first, err)
					continue
				}
				mine = append(mine, time.Since(t))
			}
			mu.Lock()
			lat = append(lat, mine...)
			errs += failed
			firstErr = cmp.Or(firstErr, first)
			mu.Unlock()
		}()
	}
	wg.Wait()
	elapsed := time.Since(began).Seconds()
	if firstErr != nil {
		fmt.Fprintf(os.Stderr, "%s, %d clients: %d errors, first: %v\n", o.name, conc, errs, firstErr)
	}
	sort.Slice(lat, func(a, b int) bool { return lat[a] < lat[b] })
	pct := func(q float64) float64 {
		if len(lat) == 0 {
			return 0
		}
		return float64(lat[min(len(lat)-1, int(q*float64(len(lat))))].Microseconds()) / 1000
	}
	return result{opsPerSec: float64(len(lat)) / elapsed, p50: pct(0.5), p99: pct(0.99), errors: errs, peakMiB: stopSampling()}
}

// sampleMemory polls the container's memory until the returned function is
// called, which returns the peak in MiB; 0 without a container.
func sampleMemory(container string) func() float64 {
	if container == "" {
		return func() float64 { return 0 }
	}
	done := make(chan struct{})
	peak := make(chan float64)
	go func() {
		max := 0.0
		for {
			out, err := exec.Command("docker", "stats", "--no-stream", "--format", "{{.MemUsage}}", container).Output()
			if err == nil {
				if fields := strings.Fields(string(out)); len(fields) > 0 {
					if v, err := parseMemory(fields[0]); err == nil && v > max {
						max = v
					}
				}
			}
			select {
			case <-done:
				peak <- max
				return
			case <-time.After(time.Second):
			}
		}
	}()
	return func() float64 { close(done); return <-peak }
}

func parseMemory(s string) (float64, error) {
	for _, u := range []struct {
		suffix string
		mib    float64
	}{{"GiB", 1024}, {"MiB", 1}, {"KiB", 1.0 / 1024}, {"B", 1.0 / (1024 * 1024)}} {
		if v, ok := strings.CutSuffix(s, u.suffix); ok {
			n, err := strconv.ParseFloat(v, 64)
			return n * u.mib, err
		}
	}
	return 0, fmt.Errorf("unknown size %q", s)
}

func setup(ctx context.Context, c *clients) error {
	_, err := c.ddb.CreateTable(ctx, &dynamodb.CreateTableInput{
		TableName: aws.String("bench"), BillingMode: ddbt.BillingModePayPerRequest,
		AttributeDefinitions: []ddbt.AttributeDefinition{
			{AttributeName: aws.String("pk"), AttributeType: ddbt.ScalarAttributeTypeS},
			{AttributeName: aws.String("sk"), AttributeType: ddbt.ScalarAttributeTypeS}},
		KeySchema: []ddbt.KeySchemaElement{
			{AttributeName: aws.String("pk"), KeyType: ddbt.KeyTypeHash},
			{AttributeName: aws.String("sk"), KeyType: ddbt.KeyTypeRange}},
	})
	if err != nil {
		return err
	}
	if err := dynamodb.NewTableExistsWaiter(c.ddb).Wait(ctx, &dynamodb.DescribeTableInput{TableName: aws.String("bench")}, time.Minute); err != nil {
		return err
	}
	for i := range 200 {
		if _, err := c.ddb.PutItem(ctx, &dynamodb.PutItemInput{TableName: aws.String("bench"), Item: map[string]ddbt.AttributeValue{
			"pk": &ddbt.AttributeValueMemberS{Value: "hot"}, "sk": &ddbt.AttributeValueMemberS{Value: fmt.Sprintf("%04d", i)},
			"v": &ddbt.AttributeValueMemberS{Value: strings.Repeat("x", 200)}}}); err != nil {
			return err
		}
	}
	if _, err := c.s3.CreateBucket(ctx, &s3.CreateBucketInput{Bucket: aws.String("bench"),
		CreateBucketConfiguration: &s3t.CreateBucketConfiguration{LocationConstraint: s3t.BucketLocationConstraintEuCentral1}}); err != nil {
		return err
	}
	for key, size := range map[string]int{"small": 4 << 10, "large": 256 << 10} {
		if _, err := c.s3.PutObject(ctx, &s3.PutObjectInput{Bucket: aws.String("bench"), Key: aws.String(key), Body: bytes.NewReader(randomBytes(size))}); err != nil {
			return err
		}
	}
	q, err := c.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("bench")})
	if err != nil {
		return err
	}
	c.queue = *q.QueueUrl
	if _, err := c.kin.CreateStream(ctx, &kinesis.CreateStreamInput{StreamName: aws.String("bench"), ShardCount: aws.Int32(4)}); err != nil {
		return err
	}
	return kinesis.NewStreamExistsWaiter(c.kin).Wait(ctx, &kinesis.DescribeStreamInput{StreamName: aws.String("bench")}, time.Minute)
}

var messageBody = strings.Repeat("stratus ", 64) // 512 bytes

func randomBytes(n int) []byte {
	b := make([]byte, n)
	_, _ = rand.Read(b)
	return b
}
