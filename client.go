package asyncsqs

import (
	"context"
	"fmt"
	"net/url"
	"sync"
	"sync/atomic"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	defaultBufferSize = 1000
	maxBatchSize      = 10
	maxPayloadBytes   = 262144
)

type sqsOp int

const (
	opSend   sqsOp = iota
	opDelete sqsOp = iota
)

// SQSClient wraps *sqs.Client from aws-sdk-go-v2
type SQSClient interface {
	SendMessageBatch(context.Context, *sqs.SendMessageBatchInput, ...func(*sqs.Options)) (*sqs.SendMessageBatchOutput, error)
	DeleteMessageBatch(context.Context, *sqs.DeleteMessageBatchInput, ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

// genericEntry for the lack of generics in Go.
type genericEntry struct {
	sendReq types.SendMessageBatchRequestEntry
	delReq  types.DeleteMessageBatchRequestEntry
}

// Config is used to configure BufferedClient.
type Config struct {
	// SQSClient abstracts *sqs.Client from aws-sdk-go-v2. You can bring your
	// own fully initialised SQS client (with required credentials, options
	// etc). This is a required field.
	SQSClient SQSClient

	// QueueURL specifies AWS SQS Queue URL for a queue.
	// This is a required field.
	QueueURL string

	// Following fields are optional.

	// SendWaitTime specifies a time limit for how long the client will
	// wait before it will dispatch accumulated send message requests
	// even if the batch isn't full. If not specified, send message
	// requests will be dispatched only when a batch is full.
	SendWaitTime time.Duration

	// SendBufferSize specifies a limit on the number of send message
	// requests that can be held in memory. If not specified, defaults
	// to 1000.
	SendBufferSize int

	// SendConcurrency limits the number of concurrent send message SQS
	// requests in progress. If not specified, defaults to SendBufferSize/10.
	SendConcurrency int

	// OnSendMessageBatch will be called with results returned by SQSClient
	// for a send message batch operation. If set, this callback function
	// needs to be goroutine safe.
	OnSendMessageBatch func(*sqs.SendMessageBatchOutput, error)

	// DeleteWaitTime specifies a time limit for how long the client will
	// wait before it will dispatch accumulated delete message requests
	// even if the batch isn't full. If not specified, delete message
	// requests will be dispatched only when a batch is full.
	DeleteWaitTime time.Duration

	// DeleteBufferSize specifies a limit on the number of delete message
	// requests that can be held in memory. If not specified, defaults
	// to 1000.
	DeleteBufferSize int

	// DeleteConcurrency limits the number of concurrent delete message SQS
	// requests in progress. If not specified, defaults to DeleteBufferSize/10.
	DeleteConcurrency int

	// OnDeleteMessageBatch will be called with results returned by SQSClient
	// for a delete message batch operation. If set, this callback function
	// needs to be goroutine safe.
	OnDeleteMessageBatch func(*sqs.DeleteMessageBatchOutput, error)
}

// Stats contains client statistics.
type Stats struct {
	MessagesSent            uint64
	MessagesDeleted         uint64
	SendMessageBatchCalls   uint64
	DeleteMessageBatchCalls uint64
}

// BufferedClient wraps aws-sdk-go-v2's sqs.Client to provide a async buffered client.
type BufferedClient struct {
	Config
	sendQueue   chan genericEntry
	deleteQueue chan genericEntry
	batchers    sync.WaitGroup
	stopped     bool
	stats       Stats
}

// NewBufferedClient creates and returns a new instance of BufferedClient. You
// will need one BufferedClient client per SQS queue. Stop() must be eventually
// called to free resources created by NewBufferedClient.
func NewBufferedClient(config Config) (*BufferedClient, error) {
	if config.SQSClient == nil {
		return nil, fmt.Errorf("config.Client cannot be nil")
	}

	if _, err := url.ParseRequestURI(config.QueueURL); err != nil {
		return nil, fmt.Errorf("invalid config.QueueURL=%s error=%w", config.QueueURL, err)
	}

	c := &BufferedClient{
		Config: config,
	}

	if c.SendBufferSize <= 0 {
		c.SendBufferSize = defaultBufferSize
	}
	c.sendQueue = make(chan genericEntry, c.SendBufferSize)

	if c.DeleteBufferSize <= 0 {
		c.DeleteBufferSize = defaultBufferSize
	}
	c.deleteQueue = make(chan genericEntry, c.DeleteBufferSize)

	if c.SendConcurrency < 1 {
		c.SendConcurrency = c.SendBufferSize / maxBatchSize
	}
	c.batchers.Add(1)
	go c.batcher(c.sendQueue, c.SendWaitTime, c.SendConcurrency, opSend, &c.batchers)

	if c.DeleteConcurrency < 1 {
		c.DeleteConcurrency = c.DeleteBufferSize / maxBatchSize
	}
	c.batchers.Add(1)
	go c.batcher(c.deleteQueue, c.DeleteWaitTime, c.DeleteConcurrency, opDelete, &c.batchers)

	return c, nil
}

// Stop stops all the batcher and dispatcher goroutines. It blocks until all
// pending requests in buffer are gracefully drained. Stop should be called
// only after calls to SendMessageAsync() and DeleteMessageAsync() have stopped.
func (c *BufferedClient) Stop() {
	if c.stopped {
		return
	}
	c.stopped = true

	close(c.sendQueue)
	close(c.deleteQueue)

	c.batchers.Wait()
}

// Stats returns client statistics.
func (c *BufferedClient) Stats() Stats {
	s := Stats{
		MessagesSent:            atomic.LoadUint64(&c.stats.MessagesSent),
		MessagesDeleted:         atomic.LoadUint64(&c.stats.MessagesDeleted),
		SendMessageBatchCalls:   atomic.LoadUint64(&c.stats.SendMessageBatchCalls),
		DeleteMessageBatchCalls: atomic.LoadUint64(&c.stats.DeleteMessageBatchCalls),
	}
	return s
}

// SendMessageAsync schedules message(s) to be sent. It blocks if the send
// buffer is full.
func (c *BufferedClient) SendMessageAsync(entries ...types.SendMessageBatchRequestEntry) error {
	if c.stopped {
		return fmt.Errorf("client stopped")
	}

	for _, entry := range entries {
		if len(*entry.MessageBody) > maxPayloadBytes/maxBatchSize {
			return fmt.Errorf("individual message size cannot exceed %d bytes", maxPayloadBytes/maxBatchSize)
		}
	}

	for _, entry := range entries {
		c.sendQueue <- genericEntry{
			sendReq: entry,
		}
	}

	return nil
}

// DeleteMessageAsync schedules message(s) to be deleted. It blocks if the delete
// buffer is full.
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
		close(jobs)        // signal dispatchers to exit
		dispatchers.Wait() // wait for dispatchers to end
	}()

	for {
		var arr [maxBatchSize]genericEntry
		var batch = arr[:0]
		for {
			select {
			case entry, ok := <-queue:
				if !ok {
					// channel closed as Stop() was called
					// drain the accumulated partial/full batch
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
					// time's up but nothing to dispatch, continue to wait
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
	resp, err := c.SQSClient.SendMessageBatch(context.TODO(), &sqs.SendMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(c.QueueURL),
	})
	atomic.AddUint64(&c.stats.SendMessageBatchCalls, 1)
	atomic.AddUint64(&c.stats.MessagesSent, uint64(len(entries)))

	if c.OnSendMessageBatch != nil {
		c.OnSendMessageBatch(resp, err)
	}
}

func (c *BufferedClient) deleteMessageBatch(entries []types.DeleteMessageBatchRequestEntry) {
	resp, err := c.SQSClient.DeleteMessageBatch(context.TODO(), &sqs.DeleteMessageBatchInput{
		Entries:  entries,
		QueueUrl: aws.String(c.QueueURL),
	})
	atomic.AddUint64(&c.stats.DeleteMessageBatchCalls, 1)
	atomic.AddUint64(&c.stats.MessagesDeleted, uint64(len(entries)))

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
