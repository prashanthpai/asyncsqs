# asyncsqs

[![Go.Dev reference](https://img.shields.io/badge/go.dev-reference-blue?logo=go)](https://pkg.go.dev/github.com/prashanthpai/asyncsqs?tab=doc)
[![Build, Unit Tests, Linters Status](https://github.com/prashanthpai/asyncsqs/actions/workflows/test.yml/badge.svg?branch=master)](https://github.com/prashanthpai/asyncsqs/actions/workflows/test.yml)
[![codecov](https://codecov.io/gh/prashanthpai/asyncsqs/branch/master/graph/badge.svg)](https://codecov.io/gh/prashanthpai/asyncsqs)
[![Go Report Card](https://goreportcard.com/badge/github.com/prashanthpai/asyncsqs?clear_cache=2)](https://goreportcard.com/report/github.com/prashanthpai/asyncsqs)
[![MIT license](https://img.shields.io/badge/license-MIT-brightgreen.svg)](https://opensource.org/licenses/MIT)

asyncsqs wraps around [SQS client](https://pkg.go.dev/github.com/aws/aws-sdk-go-v2/service/sqs#Client)
from [aws-sdk-go-v2](https://github.com/aws/aws-sdk-go-v2) to provide an async
buffered client which batches send message and delete message requests to
optimise AWS costs.

Messages can be scheduled to be sent or deleted. Requests will be dispatched
when either

* batch becomes full
* or waiting period exhausts (if configured)

...whichever occurs earlier.

**Limitation:** While asyncsqs ensures batch size doesn't exceed SQS's limit of 10
messages, it does not validate size of the payload. SQS places following limits
on batch request payload:

    The maximum allowed individual message size and the maximum total payload size
    (the sum of the individual lengths of all of the batched messages) are both
    256 KB (262,144 bytes).

This translates to an average payload limit of around 25KB per individual message.
