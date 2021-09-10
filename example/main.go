package main

import (
	"context"
	"log"
	"strconv"

	"github.com/prashanthpai/asyncsqs"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

const (
	queueURL = "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/qqqqqqqqqqqq"
)

func main() {
	// Create a SQS client with appropriate credentials/IAM role, region etc.
	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("config.LoadDefaultConfig() failed: %v", err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg)

	// Create a asyncsqs buffered client; you'd have one per SQS queue
	client, err := asyncsqs.NewBufferedClient(&asyncsqs.Config{
		SqsClient:            sqsClient,
		QueueURL:             queueURL,
		OnSendMessageBatch:   sendResponseHandler,
		OnDeleteMessageBatch: deleteResponseHandler,
	})
	if err != nil {
		log.Fatalf("asyncsqs.NewBufferedClient() failed: %v", err)
	}
	// important! Stop() ensures that requests in memory are gracefully
	// flushed/dispatched and resources like goroutines are cleaned-up
	defer client.Stop()

	for i := 0; i < 100; i++ {
		_ = client.SendMessageAsync(types.SendMessageBatchRequestEntry{
			Id:          aws.String(strconv.Itoa(i)),
			MessageBody: aws.String(strconv.Itoa(i)),
		})
	}

	// receive via normal SQS client and delete via async SQS client
	for count := 0; count < 100; {
		resp, err := sqsClient.ReceiveMessage(context.Background(), &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(queueURL),
			MaxNumberOfMessages: int32(10),
			WaitTimeSeconds:     int32(20),
		})
		if err != nil {
			log.Fatalf("sqsClient.ReceiveMessage() failed: %v", err)
		}
		log.Printf("received messages; count = %d\n", len(resp.Messages))

		for _, message := range resp.Messages {
			_ = client.DeleteMessageAsync(types.DeleteMessageBatchRequestEntry{
				Id:            aws.String(strconv.Itoa(count)),
				ReceiptHandle: message.ReceiptHandle,
			})
			count++
		}
	}
}

func sendResponseHandler(output *sqs.SendMessageBatchOutput, err error) {
	if err != nil {
		log.Printf("send returned error: %v", err)
	}
	for _, s := range output.Successful {
		log.Printf("message send successful: msg id = %s", *s.Id)
	}
	for _, f := range output.Failed {
		log.Printf("message send failed: msg id = %s", *f.Id)
	}
}

func deleteResponseHandler(output *sqs.DeleteMessageBatchOutput, err error) {
	if err != nil {
		log.Printf("send returned error: %v", err)
	}
	for _, s := range output.Successful {
		log.Printf("message delete successful: msg id = %s", *s.Id)
	}
	for _, f := range output.Failed {
		log.Printf("message delete failed: msg id = %s", *f.Id)
	}
}
