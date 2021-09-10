# asyncsqs

[![Go.Dev reference](https://img.shields.io/badge/go.dev-reference-blue?logo=go)](https://pkg.go.dev/github.com/prashanthpai/asyncsqs?tab=doc)
[![Build, Unit Tests, Linters Status](https://github.com/prashanthpai/asyncsqs/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/prashanthpai/asyncsqs/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/prashanthpai/asyncsqs/branch/master/graph/badge.svg)](https://codecov.io/gh/prashanthpai/asyncsqs)
[![Go Report Card](https://goreportcard.com/badge/github.com/prashanthpai/asyncsqs?clear_cache=2)](https://goreportcard.com/report/github.com/prashanthpai/asyncsqs)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

asyncsqs wraps around [SQS client](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/sqs#Client)
from [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) to provide an async
buffered client which batches send message and delete message requests to
**optimise AWS costs**.

Messages can be scheduled to be sent and deleted. Requests will be dispatched
when

* either batch becomes full
* or waiting period exhausts (if configured)

...**whichever occurs earlier**.

## Getting started

###### Add dependency

asyncsqs requires a Go version with [modules](https://github.com/golang/go/wiki/Modules)
support. If you're starting a new project, make sure to initialise a Go module:

```sh
$ mkdir ~/hellosqs
$ cd ~/hellosqs
$ go mod init github.com/my/hellosqs
```

And then add asyncsqs as a dependency to your existing or new project:

```sh
$ go get github.com/prashanthpai/asyncsqs
```

###### Write Code

```go
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

func main() {
	// Create a SQS client with appropriate credentials/IAM role, region etc.
	awsCfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		log.Fatalf("config.LoadDefaultConfig() failed: %v", err)
	}
	sqsClient := sqs.NewFromConfig(awsCfg)

	// Create a asyncsqs buffered client; you'd have one per SQS queue
	client, err := asyncsqs.NewBufferedClient(&asyncsqs.Config{
		SqsClient:          sqsClient,
		QueueURL:           "https://sqs.us-east-1.amazonaws.com/xxxxxxxxxxxx/qqqqqqqqqqqq",
		OnSendMessageBatch: sendResponseHandler, // register callback function (recommended)
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
			MessageBody: aws.String("hello world"),
		})
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
```

### Limitation

While asyncsqs ensures batch size doesn't exceed SQS's limit of 10 messages,
it does not validate size of the payload **yet**. SQS places following limits
on batch request payload:

    The maximum allowed individual message size and the maximum total payload size
    (the sum of the individual lengths of all of the batched messages) are both
    256 KB (262,144 bytes).

This translates to an average payload limit of around 25KB per individual message.

### TODO

* Expose concurrency/pool size as a configurable option.
* Expose some metrics/stats via an API or expvar.
* Validate payload size and perform batching adhering to SQS payload limits.
