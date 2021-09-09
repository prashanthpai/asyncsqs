package asyncsqs

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	defaultBufferSize = 1000
	maxBatchSize      = 10
)

type sqsOp int

const (
	opSend   sqsOp = iota
	opDelete sqsOp = iota
)

// SqsClient wraps *sqs.Client from aws-sdk-go-v2
type SqsClient interface {
	SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	DeleteMessageBatch(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

// compile-time guard for AWS SDK API breakages.
var _ SqsClient = sqs.New(sqs.Options{})

// genericEntry for the lack of generics in Go.
type genericEntry struct {
	sendReq types.SendMessageBatchRequestEntry
	delReq  types.DeleteMessageBatchRequestEntry
}

// Config is used to configure BufferedClient.
type Config struct {
	SqsClient SqsClient // required field
	QueueURL  string    // required field

	SendWaitTime       time.Duration
	SendBufferSize     int
	OnSendMessageBatch func(*sqs.SendMessageBatchOutput, error)

	DeleteWaitTime       time.Duration
	DeleteBufferSize     int
	OnDeleteMessageBatch func(*sqs.DeleteMessageBatchOutput, error)
}

// BufferedClient wraps aws-sdk-go-v2's sqs.Client to provide a async buffered client.
type BufferedClient struct {
	Config
	sendQueue   chan genericEntry
	deleteQueue chan genericEntry
	batchers    sync.WaitGroup
	stopped     bool
}

// NewBufferedClient creates and returns a new instance of BufferedClient.
// Stop() must be eventually called to free resources created by NewBufferedClient.
func NewBufferedClient(config *Config) (*BufferedClient, error) {
	if config == nil {
		return nil, fmt.Errorf("config cannot be nil")
	}

	if config.SqsClient == nil {
		return nil, fmt.Errorf("config.Client cannot be nil")
	}

	if _, err := url.ParseRequestURI(config.QueueURL); err != nil {
		return nil, fmt.Errorf("invalid config.QueueURL=%s error=%w", config.QueueURL, err)
	}

	c := &BufferedClient{
		Config: *config,
	}

	if c.SendBufferSize <= 0 {
		c.SendBufferSize = defaultBufferSize
	}
	c.sendQueue = make(chan genericEntry, c.SendBufferSize)

	if c.DeleteBufferSize <= 0 {
		c.DeleteBufferSize = defaultBufferSize
	}
	c.deleteQueue = make(chan genericEntry, c.DeleteBufferSize)

	c.batchers.Add(1)
	go c.batcher(c.sendQueue, c.SendWaitTime, c.SendBufferSize/maxBatchSize, opSend, &c.batchers)

	c.batchers.Add(1)
	go c.batcher(c.deleteQueue, c.DeleteWaitTime, c.DeleteBufferSize/maxBatchSize, opDelete, &c.batchers)

	return c, nil
}

// Stop stops all the batcher and dispatcher goroutines. It blocks until all
// pending requests in buffer are gracefully drained. Stop should be called
// only after calls to SendMessageAsync() and DeleteMessageAsync() have stopped.
func (c *BufferedClient) Stop() {
	c.stopped = true
	close(c.sendQueue)
	close(c.deleteQueue)
	c.batchers.Wait()
}

// SendMessageAsync schedules message(s) to be sent.
func (c *BufferedClient) SendMessageAsync(entries ...types.SendMessageBatchRequestEntry) error {
	if c.stopped {
		return fmt.Errorf("client stopped")
	}

	for _, entry := range entries {
		c.sendQueue <- genericEntry{
			sendReq: entry,
		}
	}

	return nil
}

// DeleteMessageAsync schedules message(s) to be deleted.
func (c *BufferedClient) DeleteMessageAsync(entries ...types.DeleteMessageBatchRequestEntry) error {
	if c.stopped {
		return fmt.Errorf("client stopped")
	}

	for _, entry := range entries {
		c.deleteQueue <- genericEntry{
			delReq: entry,
		}
	}

	return nil
}

// batcher batches multiple send and delete requests to be dispatched in batches.
func (c *BufferedClient) batcher(queue chan genericEntry, waitTime time.Duration, concurrency int, op sqsOp, wg *sync.WaitGroup) {
	defer wg.Done()

	var ticker *time.Ticker
	if waitTime <= 0 {
		ticker = disabledTicker()
	} else {
		ticker = time.NewTicker(waitTime)
		defer ticker.Stop()
	}

	jobs := make(chan []genericEntry)
	var dispatchers sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		dispatchers.Add(1)
		go c.dispatcher(jobs, op, &dispatchers)
	}

	defer func() {
		close(jobs)
		dispatchers.Wait()
	}()

	for {
		var arr [maxBatchSize]genericEntry
		var batch = arr[:0]
		for {
			select {
			case entry, ok := <-queue:
				if !ok {
					// channel closed as Stop() was called
					// drain whatever partial batch was accumulated
					if len(batch) > 0 {
						jobs <- batch
					}
					return
				}
				batch = append(batch, entry)
				if len(batch) != maxBatchSize {
					// batch hasn't filled up yet, continue to wait
					continue
				}
			case <-ticker.C:
				if len(batch) < 1 {
					// time's up but nothing to send, continue to wait
					continue
				}
			}
			jobs <- batch
			break // break inner loop, create a new batch
		}
	}
}

func (c *BufferedClient) dispatcher(batches chan []genericEntry, op sqsOp, wg *sync.WaitGroup) {
	defer wg.Done()

	for batch := range batches {
		c.dispatchBatch(batch, op)
	}
}

func (c *BufferedClient) dispatchBatch(batch []genericEntry, op sqsOp) {
	switch op {
	case opSend:
		var arr [maxBatchSize]types.SendMessageBatchRequestEntry
		var entries = arr[:0]

		for _, ge := range batch {
			entries = append(entries, ge.sendReq)
		}
		c.sendMessageBatch(entries)
	case opDelete:
		var arr [maxBatchSize]types.DeleteMessageBatchRequestEntry
		var entries = arr[:0]

		for _, ge := range batch {
			entries = append(entries, ge.delReq)
		}
		c.deleteMessageBatch(entries)
	}
}

func (c *BufferedClient) sendMessageBatch(entries []types.SendMessageBatchRequestEntry) {
	resp, err := c.SqsClient.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(c.QueueURL),
	})

	if c.OnSendMessageBatch != nil {
		c.OnSendMessageBatch(resp, err)
	}
}

func (c *BufferedClient) deleteMessageBatch(entries []types.DeleteMessageBatchRequestEntry) {
	resp, err := c.SqsClient.DeleteMessageBatch(context.TODO(), &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(c.QueueURL),
	})

	if c.OnDeleteMessageBatch != nil {
		c.OnDeleteMessageBatch(resp, err)
	}
}

// disabledTicker returns a ticker that is stopped and shall never tick.
func disabledTicker() *time.Ticker {
	ticker := time.NewTicker(1 * time.Hour)
	ticker.Stop()
	return ticker
}
