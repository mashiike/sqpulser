package sqpulser_test

import (
	"encoding/json"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/mashiike/sqpulser"
	"github.com/stretchr/testify/require"
)

// sampleEvent is obtained from the following, with MessageAttribute
// https://docs.aws.amazon.com/lambda/latest/dg/with-sqs-example.html
const sampleEvent = `
{
	"Records": [
		{
			"messageId": "059f36b4-87a3-44ab-83d2-661975830a7d",
			"receiptHandle": "AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a...",
			"body": "test",
			"attributes": {
				"ApproximateReceiveCount": "1",
				"SentTimestamp": "1545082649183",
				"SenderId": "AIDAIENQZJOLO23YVJ4VO",
				"ApproximateFirstReceiveTimestamp": "1545082649185"
			},
			"messageAttributes": {
				"OriginalMessageID" : {
					"stringValue" : "d3217307-c31f-42ad-a235-2d80def3f919",
					"dataType" : "String"
				  },
				  "OriginalSentTimestamp" : {
					"stringValue" : "1545081649183",
					"dataType" : "Number"
				  }
			},
			"md5OfBody": "098f6bcd4621d373cade4e832627b4f6",
			"eventSource": "aws:sqs",
			"eventSourceARN": "arn:aws:sqs:us-east-2:123456789012:my-queue",
			"awsRegion": "us-east-2"
		}
	]
}
`

func TestSQSEventUnmarshal(t *testing.T) {
	var actual sqpulser.SQSEvent
	err := json.Unmarshal([]byte(sampleEvent), &actual)
	require.NoError(t, err)
	expected := &sqpulser.SQSEvent{
		Records: []types.Message{
			{
				MessageId:     aws.String("059f36b4-87a3-44ab-83d2-661975830a7d"),
				ReceiptHandle: aws.String("AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."),
				Body:          aws.String("test"),
				Attributes: map[string]string{
					"ApproximateReceiveCount":          "1",
					"SentTimestamp":                    "1545082649183",
					"SenderId":                         "AIDAIENQZJOLO23YVJ4VO",
					"ApproximateFirstReceiveTimestamp": "1545082649185",
				},
				MessageAttributes: map[string]types.MessageAttributeValue{
					sqpulser.OriginalMessageIDAttributeKey: {
						DataType:    aws.String("String"),
						StringValue: aws.String("d3217307-c31f-42ad-a235-2d80def3f919"),
					},
					sqpulser.OriginalMessageSentTimestampAttributeKey: {
						DataType:    aws.String("Number"),
						StringValue: aws.String("1545081649183"),
					},
				},
				MD5OfBody: aws.String("098f6bcd4621d373cade4e832627b4f6"),
			},
		},
	}
	require.EqualValues(t, expected, &actual)
}
