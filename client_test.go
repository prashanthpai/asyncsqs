package asyncsqs

import (
	"strconv"
	"sync"
	"testing"

	"github.com/prashanthpai/asyncsqs/mocks"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
)

func TestNew(t *testing.T) {
	assert := require.New(t)

	tcs := []*Config{
		nil,
		{},
		{SqsClient: new(mocks.SqsClient)},
		{SqsClient: new(mocks.SqsClient), QueueURL: "invalid URL"},
	}

	for _, tc := range tcs {
		if _, err := NewBufferedClient(tc); err == nil {
			t.Error("expected error")
		}
	}

	c, err := NewBufferedClient(&Config{
		SqsClient: new(mocks.SqsClient),
		QueueURL:  "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/some-queue",
	})
	assert.NotNil(c)
	assert.Nil(err)
	c.Stop()

	// Async funcs should return error after stopping
	assert.NotNil(c.SendMessageAsync())
	assert.NotNil(c.DeleteMessageAsync())
}

func TestSendBatchNoWaitTime(t *testing.T) {
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
			mockSqsClient := new(mocks.SqsClient)
			for i := 0; i < numSqsCalls; i++ {
				mockSqsClient.On("SendMessageBatch",
					mock.AnythingOfType("*context.emptyCtx"),
					mock.AnythingOfType("*sqs.SendMessageBatchInput")).
					Return(nil, nil)
				mockSqsClient.On("DeleteMessageBatch",
					mock.AnythingOfType("*context.emptyCtx"),
					mock.AnythingOfType("*sqs.DeleteMessageBatchInput")).
					Return(nil, nil)
			}

			client, err := NewBufferedClient(&Config{
				SqsClient: mockSqsClient,
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

			wg.Wait() // all async calls have been accepted

			client.Stop() // stop client; test that draining of remaining requests work

			// wait for send callbacks to finish
			sendCbks.Wait()

			// wait for delete callbacks to finish
			delCbks.Wait()

			// assert SQS requests made
			mockSqsClient.AssertExpectations(t)
		})
	}
}
