package sqpulser

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Songmu/flextime"
	"github.com/aws/aws-lambda-go/lambda"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// Option represents option values of app
type Option struct {
	IncomingQueueURL  string
	OutgoingQueueURL  string
	IncomingQueueName string
	OutgoingQueueName string
	EmitInterval      time.Duration
	Offset            time.Duration
}

type SQSClient interface {
	SendMessage(ctx context.Context, params *sqs.SendMessageInput, optFns ...func(*sqs.Options)) (*sqs.SendMessageOutput, error)
	ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	GetQueueUrl(ctx context.Context, params *sqs.GetQueueUrlInput, optFns ...func(*sqs.Options)) (*sqs.GetQueueUrlOutput, error)
}

type App struct {
	client SQSClient
	opt    *Option
}

func New(ctx context.Context, opt *Option, optFns ...func(*config.LoadOptions) error) (*App, error) {
	c, err := config.LoadDefaultConfig(ctx, optFns...)
	if err != nil {
		return nil, err
	}
	client := sqs.NewFromConfig(c)
	return NewWithClient(ctx, client, opt)
}

func NewWithClient(ctx context.Context, client SQSClient, opt *Option) (*App, error) {
	if opt.IncomingQueueURL == "" && opt.IncomingQueueName == "" {
		return nil, errors.New("either incoming queue url or incoming quene name is required")
	}
	if opt.IncomingQueueURL == "" {
		log.Printf("[info] try get incoming queue url: queue name `%s`", opt.IncomingQueueName)
		output, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: aws.String(opt.IncomingQueueName),
		})
		if err != nil {
			return nil, fmt.Errorf("can not get incoming queue url: %w", err)
		}
		opt.IncomingQueueURL = *output.QueueUrl
	}
	if opt.OutgoingQueueURL == "" && opt.OutgoingQueueName == "" {
		return nil, errors.New("either outgoing queue url or outgoing quene name is required")
	}
	if opt.OutgoingQueueURL == "" {
		log.Printf("[info] try get outgoing queue url: queue name `%s`", opt.OutgoingQueueName)
		output, err := client.GetQueueUrl(ctx, &sqs.GetQueueUrlInput{
			QueueName: aws.String(opt.OutgoingQueueName),
		})
		if err != nil {
			return nil, fmt.Errorf("can not get outgoing queue url: %w", err)
		}
		opt.OutgoingQueueURL = *output.QueueUrl
	}
	app := &App{
		client: client,
		opt:    opt,
	}
	return app, nil
}

func (app *App) Run(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	if strings.HasPrefix(os.Getenv("AWS_EXECUTION_ENV"), "AWS_Lambda") || os.Getenv("AWS_LAMBDA_RUNTIME_API") != "" {
		log.Printf("[info] start lambda handler")
		lambda.Start(app.LambdaHandler)
		return nil
	}
	return app.run(ctx)
}

func (app *App) run(ctx context.Context) error {
	var trapSignals = []os.Signal{
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
	}
	ctx, stop := signal.NotifyContext(ctx, trapSignals...)
	defer stop()

	log.Printf("[info] start polling: %s ", app.opt.IncomingQueueURL)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
		}
		output, err := app.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			MaxNumberOfMessages:   1,
			QueueUrl:              aws.String(app.opt.IncomingQueueURL),
			MessageAttributeNames: []string{"All"},
			AttributeNames:        []types.QueueAttributeName{"All"},
		})
		if err != nil {
			if errors.Is(err, context.Canceled) && errors.Is(err, context.DeadlineExceeded) {
				log.Printf("[warn] recive message: %v", err)
				time.Sleep(time.Second)
			}
			continue
		}
		for _, msg := range output.Messages {
			log.Printf("[info][%s] recive message handle=%s", *msg.MessageId, *msg.ReceiptHandle)
			m := msg
			if err := app.HandleMessage(ctx, &m); err != nil {
				log.Printf("[error][%s] failed to handle message. %v", *msg.MessageId, err)
				continue
			}
			if _, err := app.client.DeleteMessage(ctx, &sqs.DeleteMessageInput{
				QueueUrl:      aws.String(app.opt.IncomingQueueURL),
				ReceiptHandle: aws.String(*m.ReceiptHandle),
			}); err != nil {
				log.Printf("[error][%s] failed to delete message:%v, handle=%s", *msg.MessageId, err, *msg.ReceiptHandle)
				continue
			}
			log.Printf("[info][%s] success", *msg.MessageId)
		}
	}
}

const (
	OriginalMessageSentTimestampAttributeKey = "OriginalSentTimestamp"
	OriginalMessageIDAttributeKey            = "OriginalMessageID"
	sqsMaxDelaySeconds                       = 900
)

type OriginalAttributes struct {
	MessageID     string
	SentTimestamp int64
}

func (app *App) HandleMessage(ctx context.Context, msg *types.Message) error {
	log.Printf("[debug][%s] received message: %v", *msg.MessageId, *msg)

	originalAttr, err := ExtructOriginalAttribute(msg)
	if err != nil {
		return fmt.Errorf("extruct original attribute: %w", err)
	}
	if originalAttr == nil {
		log.Printf("[debug][%s] handle 1st time message", *msg.MessageId)
		sentTimestamp, err := ExtructSentTimestamp(msg)
		if err != nil {
			return fmt.Errorf("extruct sent timestamp: %w", err)
		}
		log.Printf("[info][%s] handle 1st time message, sentTimestamp=%d", *msg.MessageId, sentTimestamp)
		originalAttr = &OriginalAttributes{
			MessageID:     *msg.MessageId,
			SentTimestamp: sentTimestamp,
		}
	} else {
		log.Printf("[info][%s] handle extended message, originalMessageID=%s originalSentTimestamp=%d", *msg.MessageId, originalAttr.MessageID, originalAttr.SentTimestamp)
	}
	input := &sqs.SendMessageInput{
		MessageBody:       msg.Body,
		MessageAttributes: msg.MessageAttributes,
	}
	input.MessageAttributes = originalAttr.SetMessageAttribute(input.MessageAttributes)
	delay := originalAttr.DelayDuration(app.opt.EmitInterval, app.opt.Offset)
	if delay <= sqsMaxDelaySeconds*time.Second {
		log.Printf("[info][%s] no extended, ready to emit delay=%s", *msg.MessageId, delay)
		input.DelaySeconds = int32(delay.Seconds())
		input.QueueUrl = aws.String(app.opt.OutgoingQueueURL)
		output, err := app.client.SendMessage(ctx, input)
		if err != nil {
			return fmt.Errorf("send message to %s: %w", app.opt.OutgoingQueueURL, err)
		}
		log.Printf("[info][%s] send to %s, message id=%s", *msg.MessageId, app.opt.OutgoingQueueURL, *output.MessageId)
	} else {
		log.Printf("[info][%s] need extended, resend queue totalDelay=%s", *msg.MessageId, delay)
		input.DelaySeconds = int32(sqsMaxDelaySeconds)
		input.QueueUrl = aws.String(app.opt.IncomingQueueURL)
		output, err := app.client.SendMessage(ctx, input)
		if err != nil {
			return fmt.Errorf("send message to %s: %w", app.opt.IncomingQueueURL, err)
		}
		log.Printf("[info][%s] send to %s, message id=%s", *msg.MessageId, app.opt.IncomingQueueURL, *output.MessageId)
	}
	return nil
}

func ExtructOriginalAttribute(msg *types.Message) (*OriginalAttributes, error) {
	if msg.MessageAttributes == nil {
		return nil, nil
	}
	originalMessageID, ok := msg.MessageAttributes[OriginalMessageIDAttributeKey]
	if !ok {
		return nil, nil
	}
	if originalMessageID.DataType == nil || *originalMessageID.DataType != "String" {
		return nil, fmt.Errorf("original message id attribute type is missmatch:%v", originalMessageID.DataType)
	}
	if originalMessageID.StringValue == nil || *originalMessageID.StringValue == "" {
		return nil, fmt.Errorf("original message id attribute value is empty")
	}
	originalSentTimestampString, ok := msg.MessageAttributes[OriginalMessageSentTimestampAttributeKey]
	if !ok {
		return nil, fmt.Errorf("original message id %s, but not set sent timestamp", *originalMessageID.StringValue)
	}
	if originalSentTimestampString.DataType == nil || *originalSentTimestampString.DataType != "Number" {
		return nil, fmt.Errorf("original sent timestamp attribute type is missmatch:%v", originalSentTimestampString.DataType)
	}
	if originalSentTimestampString.StringValue == nil {
		return nil, fmt.Errorf("original sent timestamep attribute value is empty")
	}
	originalSentTimestamp, err := strconv.ParseInt(*originalSentTimestampString.StringValue, 10, 64)
	if err != nil {
		return nil, fmt.Errorf("original sent timestamep attribute value parse failed: %w", err)
	}
	originalAttr := &OriginalAttributes{
		MessageID:     *originalMessageID.StringValue,
		SentTimestamp: originalSentTimestamp,
	}
	return originalAttr, nil
}

func ExtructSentTimestamp(msg *types.Message) (int64, error) {
	if msg.Attributes == nil {
		return 0, errors.New("attributes not found")
	}
	sentTimestampString, ok := msg.Attributes["SentTimestamp"]
	if !ok {
		return 0, errors.New("attribute SentTimestamp not found")
	}
	sentTimestamp, err := strconv.ParseInt(sentTimestampString, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("sent timestamep attribute value parse failed: %w", err)
	}
	return sentTimestamp, nil
}

func (attr *OriginalAttributes) SentTime() time.Time {
	return time.UnixMilli(attr.SentTimestamp)
}

func (attr *OriginalAttributes) EmitTime(emitInterval, offset time.Duration) time.Time {
	return attr.SentTime().Truncate(emitInterval).Add(emitInterval).Add(offset)
}

func (attr *OriginalAttributes) DelayDuration(emitInterval, offset time.Duration) time.Duration {
	now := flextime.Now()
	emitTime := attr.EmitTime(emitInterval, offset)
	delay := emitTime.Sub(now)
	if delay < 0 {
		return 0
	}
	return delay
}

func (attr *OriginalAttributes) SetMessageAttribute(attributes map[string]types.MessageAttributeValue) map[string]types.MessageAttributeValue {
	if attributes == nil {
		attributes = make(map[string]types.MessageAttributeValue, 2)
	}
	attributes[OriginalMessageIDAttributeKey] = types.MessageAttributeValue{
		DataType:    aws.String("String"),
		StringValue: aws.String(attr.MessageID),
	}
	attributes[OriginalMessageSentTimestampAttributeKey] = types.MessageAttributeValue{
		DataType:    aws.String("Number"),
		StringValue: aws.String(fmt.Sprintf("%d", attr.SentTimestamp)),
	}
	return attributes
}
