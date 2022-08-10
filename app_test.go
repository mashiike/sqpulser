package sqpulser_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/mashiike/sqpulser"
	"github.com/stretchr/testify/require"
)

func TestExtructOriginalAttribute(t *testing.T) {
	cases := []struct {
		name      string
		msg       types.Message
		errString string
		attr      *sqpulser.OriginalAttributes
	}{
		{
			name: "empty",
			msg: types.Message{
				MessageId:     aws.String("059f36b4-87a3-44ab-83d2-661975830a7d"),
				ReceiptHandle: aws.String("AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."),
				Body:          aws.String("test"),
				Attributes: map[string]string{
					"ApproximateReceiveCount":          "1",
					"SentTimestamp":                    "1545082649183",
					"SenderId":                         "AIDAIENQZJOLO23YVJ4VO",
					"ApproximateFirstReceiveTimestamp": "1545082649185",
				},
				MD5OfBody: aws.String("098f6bcd4621d373cade4e832627b4f6"),
			},
		},
		{
			name: "success",
			msg: types.Message{
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
			attr: &sqpulser.OriginalAttributes{
				MessageID:     "d3217307-c31f-42ad-a235-2d80def3f919",
				SentTimestamp: 1545081649183,
			},
		},
		{
			name: "missing sentTimestamp",
			msg: types.Message{
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
				},
				MD5OfBody: aws.String("098f6bcd4621d373cade4e832627b4f6"),
			},
			errString: "original message id d3217307-c31f-42ad-a235-2d80def3f919, but not set sent timestamp",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			attr, err := sqpulser.ExtructOriginalAttribute(&c.msg)
			if c.errString == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, c.errString)
			}
			require.EqualValues(t, c.attr, attr)
		})
	}
}

func TestExtructSentTimestamp(t *testing.T) {
	cases := []struct {
		name          string
		msg           types.Message
		errString     string
		sentTimestamp int64
	}{
		{
			name: "empty",
			msg: types.Message{
				MessageId:     aws.String("059f36b4-87a3-44ab-83d2-661975830a7d"),
				ReceiptHandle: aws.String("AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."),
				Body:          aws.String("test"),
				MD5OfBody:     aws.String("098f6bcd4621d373cade4e832627b4f6"),
			},
			errString: "attributes not found",
		},
		{
			name: "success",
			msg: types.Message{
				MessageId:     aws.String("059f36b4-87a3-44ab-83d2-661975830a7d"),
				ReceiptHandle: aws.String("AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."),
				Body:          aws.String("test"),
				Attributes: map[string]string{
					"ApproximateReceiveCount":          "1",
					"SentTimestamp":                    "1545082649183",
					"SenderId":                         "AIDAIENQZJOLO23YVJ4VO",
					"ApproximateFirstReceiveTimestamp": "1545082649185",
				},
				MD5OfBody: aws.String("098f6bcd4621d373cade4e832627b4f6"),
			},
			sentTimestamp: 1545082649183,
		},
		{
			name: "missing sentTimestamp",
			msg: types.Message{
				MessageId:     aws.String("059f36b4-87a3-44ab-83d2-661975830a7d"),
				ReceiptHandle: aws.String("AQEBwJnKyrHigUMZj6rYigCgxlaS3SLy0a..."),
				Body:          aws.String("test"),
				Attributes: map[string]string{
					"SenderId": "AIDAIENQZJOLO23YVJ4VO",
				},
				MD5OfBody: aws.String("098f6bcd4621d373cade4e832627b4f6"),
			},
			errString: "attribute SentTimestamp not found",
		},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			sentTimestamp, err := sqpulser.ExtructSentTimestamp(&c.msg)
			if c.errString == "" {
				require.NoError(t, err)
			} else {
				require.EqualError(t, err, c.errString)
			}
			require.EqualValues(t, c.sentTimestamp, sentTimestamp)
		})
	}
}

func TestOriginalAttrSentTime(t *testing.T) {
	attr := &sqpulser.OriginalAttributes{
		MessageID:     "d3217307-c31f-42ad-a235-2d80def3f919",
		SentTimestamp: 1545081649183,
	}
	require.EqualValues(t, "2018-12-17T21:20:49Z", attr.SentTime().UTC().Format(time.RFC3339))
}

func Must[T any](t T, err error) T {
	if err != nil {
		panic(err)
	}
	return t
}

func TestOriginalAttrCore(t *testing.T) {
	restore := flextime.Fix(Must(time.Parse(time.RFC3339, "2018-12-17T21:28:00Z")))
	defer restore()

	cases := []struct {
		now              time.Time
		attr             *sqpulser.OriginalAttributes
		emitInterval     time.Duration
		offset           time.Duration
		expectedEmitTime string
		expectedDelay    time.Duration
	}{
		{
			attr: &sqpulser.OriginalAttributes{
				MessageID:     "d3217307-c31f-42ad-a235-2d80def3f919",
				SentTimestamp: Must(time.Parse(time.RFC3339, "2018-12-17T21:20:49Z")).UnixMilli(),
			},
			emitInterval:     15 * time.Minute,
			expectedEmitTime: "2018-12-17T21:30:00Z",
			expectedDelay:    2 * time.Minute,
		},
		{
			attr: &sqpulser.OriginalAttributes{
				MessageID:     "d3217307-c31f-42ad-a235-2d80def3f919",
				SentTimestamp: Must(time.Parse(time.RFC3339, "2018-12-17T21:20:49Z")).UnixMilli(),
			},
			emitInterval:     15 * time.Minute,
			offset:           5 * time.Minute,
			expectedEmitTime: "2018-12-17T21:35:00Z",
			expectedDelay:    7 * time.Minute,
		},
		{
			attr: &sqpulser.OriginalAttributes{
				MessageID:     "d3217307-c31f-42ad-a235-2d80def3f919",
				SentTimestamp: Must(time.Parse(time.RFC3339, "2018-12-17T21:20:49Z")).UnixMilli(),
			},
			emitInterval:     1 * time.Hour,
			expectedEmitTime: "2018-12-17T22:00:00Z",
			expectedDelay:    32 * time.Minute,
		},
	}

	for _, c := range cases {
		t.Run(fmt.Sprintf("%s__%s", c.attr.SentTime(), c.emitInterval), func(t *testing.T) {
			if !c.now.IsZero() {
				restore := flextime.Fix(Must(time.Parse(time.RFC3339, "2018-12-17T21:28:00Z")))
				defer restore()
			}
			require.EqualValues(t, c.expectedEmitTime, c.attr.EmitTime(c.emitInterval, c.offset).UTC().Format(time.RFC3339))
			require.EqualValues(t, c.expectedDelay, c.attr.DelayDuration(c.emitInterval, c.offset))
		})
	}
}
