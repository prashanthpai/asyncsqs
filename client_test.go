package asyncsqs

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	assert := require.New(t)

	tcs := []Config{
		{},
		{SQSClient: new(mockSQSClient)},
		{SQSClient: new(mockSQSClient), QueueURL: "invalid URL"},
	}

	for _, tc := range tcs {
		if _, err := NewBufferedClient(tc); err == nil {
			t.Error("expected error")
		}
	}

	c, err := NewBufferedClient(Config{
		SQSClient: new(mockSQSClient),
		QueueURL:  "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/some-queue",
	})
	assert.NotNil(c)
	assert.Nil(err)

	// check defaults
	assert.Equal(c.SendBufferSize, defaultBufferSize)
	assert.Equal(c.DeleteBufferSize, defaultBufferSize)
	assert.Equal(c.SendConcurrency, c.SendBufferSize/maxBatchSize)
	assert.Equal(c.DeleteConcurrency, c.DeleteBufferSize/maxBatchSize)

	c.Stop()

	// Async funcs should return error after stopping
	assert.NotNil(c.SendMessageAsync())
	assert.NotNil(c.DeleteMessageAsync())
}

func TestAsyncBatchNoWaitTime(t *testing.T) {
	testNums := []int{1, 9, 10, 11, 99, 100, 101, 109}
	for _, tc := range testNums {
		tc := tc // capture range variable
		t.Run(strconv.Itoa(tc), func(t *testing.T) {
			assert := require.New(t)

			numMsgs := tc
			// if num of messages = 109, it should result in 11 SQS requests
			//  - 10 SQS requests as and when batch fills up
			//  - 1 more SQS request to drain remaining 9 when Stop() is called
			numSqsCalls := (numMsgs / maxBatchSize)
			if numMsgs%maxBatchSize != 0 {
				numSqsCalls++
			}

			sendCbks := &sync.WaitGroup{}
			sendCbks.Add(numSqsCalls)

			delCbks := &sync.WaitGroup{}
			delCbks.Add(numSqsCalls)

			// mock SQS calls
			mockSQSClient := new(mockSQSClient)
			mockSQSClient.On("SendMessageBatch",
				mock.AnythingOfType("*context.emptyCtx"), mock.AnythingOfType("*sqs.SendMessageBatchInput")).
				Return(nil, nil).
				Times(numSqsCalls)
			mockSQSClient.On("DeleteMessageBatch",
				mock.AnythingOfType("*context.emptyCtx"), mock.AnythingOfType("*sqs.DeleteMessageBatchInput")).
				Return(nil, nil).
				Times(numSqsCalls)

			client, err := NewBufferedClient(Config{
				SQSClient: mockSQSClient,
				QueueURL:  "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/some-queue",
				OnSendMessageBatch: func(output *sqs.SendMessageBatchOutput, err error) {
					assert.Nil(output)
					assert.Nil(err)
					sendCbks.Done()
				},
				OnDeleteMessageBatch: func(output *sqs.DeleteMessageBatchOutput, err error) {
					assert.Nil(output)
					assert.Nil(err)
					delCbks.Done()
				},
			})
			assert.NotNil(client)
			assert.Nil(err)
			defer client.Stop()

			wg := &sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < numMsgs; i++ {
					_ = client.SendMessageAsync(types.SendMessageBatchRequestEntry{
						Id:          aws.String(strconv.Itoa(i)),
						MessageBody: aws.String(strconv.Itoa(i)),
					})
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < numMsgs; i++ {
					_ = client.DeleteMessageAsync(types.DeleteMessageBatchRequestEntry{
						Id: aws.String(strconv.Itoa(i)),
					})
				}
			}()

			wg.Wait() // wait for all async calls to be accepted

			client.Stop() // stop client; test that draining of remaining requests work

			// wait for send callbacks to finish
			sendCbks.Wait()

			// wait for delete callbacks to finish
			delCbks.Wait()

			// assert SQS requests made
			mockSQSClient.AssertExpectations(t)

			// assert stats
			s := client.Stats()
			assert.Equal(s.MessagesSent, uint64(numMsgs))
			assert.Equal(s.MessagesDeleted, uint64(numMsgs))
			assert.Equal(s.SendMessageBatchCalls, uint64(numSqsCalls))
			assert.Equal(s.DeleteMessageBatchCalls, uint64(numSqsCalls))
		})
	}
}

func TestAsyncBatchWithWaitTime(t *testing.T) {
	testNums := []int{1, 9, 10, 11, 31}
	for _, tc := range testNums {
		tc := tc // capture range variable
		t.Run(strconv.Itoa(tc), func(t *testing.T) {
			assert := require.New(t)

			// messages to be sent or deleted - N each
			numMsgs := tc
			msgs := &sync.WaitGroup{}
			msgs.Add(numMsgs * 2)

			// how long do we wait until we ship the batch (even if its not full)
			waitTime := 250 * time.Millisecond

			// delay between successive calls to SendMessageAsync or DeleteMessageAsync
			// should transate to roughly 5 or less in a batch but not more
			delayBetweenAsyncCalls := 50 * time.Millisecond

			// setup to record every batch size during dispatch
			batchSizes := make([]int, 0, numMsgs)
			batchMu := &sync.RWMutex{}
			recordBatchSize := func(size int) {
				batchMu.Lock()
				batchSizes = append(batchSizes, size)
				batchMu.Unlock()
				for i := 0; i < size; i++ {
					msgs.Done()
				}
			}

			// mock SQS calls
			mockSQSClient := new(mockSQSClient)
			mockSQSClient.On("SendMessageBatch",
				mock.AnythingOfType("*context.emptyCtx"),
				mock.AnythingOfType("*sqs.SendMessageBatchInput")).
				Return(nil, nil).
				Maybe().                        // flexible no. of calls
				Run(func(args mock.Arguments) { // hook to inspect and record batch size
					recordBatchSize(len(args.Get(1).(*sqs.SendMessageBatchInput).Entries))
				})
			mockSQSClient.On("DeleteMessageBatch",
				mock.AnythingOfType("*context.emptyCtx"),
				mock.AnythingOfType("*sqs.DeleteMessageBatchInput")).
				Return(nil, nil).
				Maybe().                        // flexible no. of calls
				Run(func(args mock.Arguments) { // hook to inspect and record batch size
					recordBatchSize(len(args.Get(1).(*sqs.DeleteMessageBatchInput).Entries))
				})

			client, err := NewBufferedClient(Config{
				SQSClient:      mockSQSClient,
				QueueURL:       "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/some-queue",
				SendWaitTime:   waitTime,
				DeleteWaitTime: waitTime,
			})
			assert.NotNil(client)
			assert.Nil(err)
			defer client.Stop()

			wg := &sync.WaitGroup{}

			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < numMsgs; i++ {
					_ = client.SendMessageAsync(types.SendMessageBatchRequestEntry{
						Id:          aws.String(strconv.Itoa(i)),
						MessageBody: aws.String(strconv.Itoa(i)),
					})
					time.Sleep(delayBetweenAsyncCalls)
				}
			}()

			wg.Add(1)
			go func() {
				defer wg.Done()
				for i := 0; i < numMsgs; i++ {
					_ = client.DeleteMessageAsync(types.DeleteMessageBatchRequestEntry{
						Id: aws.String(strconv.Itoa(i)),
					})
					time.Sleep(delayBetweenAsyncCalls)
				}
			}()

			wg.Wait() // wait for all async calls to be accepted

			client.Stop() // stop client; test that draining of remaining requests work

			msgs.Wait() // wait for all batches to be dispatched

			// assert SQS requests made
			mockSQSClient.AssertExpectations(t)

			totalDispatched := 0
			for _, bs := range batchSizes {
				// assert that neither empty nor full batch is ever dispatched
				assert.True(bs > 0 && bs < maxBatchSize)
				// keep track of total calls made for later assertion
				totalDispatched += bs
			}

			// assert that all requests made through - numMsgs for each of sends and deletes
			assert.Equal(totalDispatched, 2*numMsgs)

			// assert stats
			s := client.Stats()
			assert.Equal(s.MessagesSent, uint64(numMsgs))
			assert.Equal(s.MessagesDeleted, uint64(numMsgs))
			assert.Equal(s.SendMessageBatchCalls+s.DeleteMessageBatchCalls, uint64(len(batchSizes)))
		})
	}
}
