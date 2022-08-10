package sqpulser

import (
	"context"
	"errors"
	"fmt"
	"log"

	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

type SQSEvent struct {
	Records []types.Message
}
type SQSBatchResponse struct {
	BatchItemFailures []BatchItemFailureItem `json:"batchItemFailures,omitempty"`
}

type BatchItemFailureItem struct {
	ItemIdentifier string `json:"itemIdentifier"`
}

func (app *App) LambdaHandler(ctx context.Context, event *SQSEvent) (*SQSBatchResponse, error) {
	resp := &SQSBatchResponse{
		BatchItemFailures: nil,
	}
	for _, record := range event.Records {
		if record.MessageId == nil {
			return nil, errors.New("message id is empty, maybe not sqs event")
		}
		func() {
			defer func() {
				if err := recover(); err != nil {
					log.Printf("[error][%s] handle message panic %v", *record.MessageId, err)
					resp.BatchItemFailures = append(resp.BatchItemFailures, BatchItemFailureItem{
						ItemIdentifier: *record.MessageId,
					})
				}
			}()
			if err := app.HandleMessage(ctx, &record); err != nil {
				log.Printf("[error][%s] %v", *record.MessageId, err)
				resp.BatchItemFailures = append(resp.BatchItemFailures, BatchItemFailureItem{
					ItemIdentifier: *record.MessageId,
				})
			}
		}()
	}
	if len(event.Records) == 1 && len(resp.BatchItemFailures) == 1 {
		//no batch
		return nil, fmt.Errorf("failure message id: %s", resp.BatchItemFailures[0].ItemIdentifier)
	}
	return resp, nil
}
