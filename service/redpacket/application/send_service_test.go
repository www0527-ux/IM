package application

import (
	"context"
	"errors"
	"testing"
)

func TestSendDoesNotReportSuccessBeforeCoreTransactionExists(t *testing.T) {
	service := NewSendService(SendDependencies{})

	result, err := service.Send(context.Background(), SendCommand{})
	if !errors.Is(err, ErrSendCoreNotImplemented) {
		t.Fatalf("Send() error = %v, want ErrSendCoreNotImplemented", err)
	}
	if result != (SendResult{}) {
		t.Fatalf("Send() result = %+v, want zero result", result)
	}
}
