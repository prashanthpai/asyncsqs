package asyncsqs

import (
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
)

func TestNew(t *testing.T) {
	tcs := []*Config{
		nil,
		{},
		{SqsClient: sqs.New(sqs.Options{})},
		{SqsClient: sqs.New(sqs.Options{}), QueueURL: "invalid URL"},
	}

	for _, tc := range tcs {
		if _, err := NewBufferedClient(tc); err == nil {
			t.Error("expected error")
		}
	}

	c, err := NewBufferedClient(&Config{
		SqsClient: sqs.New(sqs.Options{}),
		QueueURL:  "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/some-queue",
	})
	if c == nil {
		t.Error("client is nil")
	}
	if err != nil {
		t.Error("unexpected error")
	}
	defer c.Stop()
}
