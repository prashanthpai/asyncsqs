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
	assert := require.New(t)

	numMsgs := 100
	numSqsCalls := numMsgs / maxBatchSize // should result in 10 requests

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

	// wait for send callbacks to finish
	sendCbks.Wait()

	// wait for delete callbacks to finish
	delCbks.Wait()

	// assert SQS requests made
	mockSqsClient.AssertExpectations(t)
}
