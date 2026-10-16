package application

import (
	"context"
	"errors"
	"testing"
)

func TestSendRejectsMissingDependencies(t *testing.T) {
	service := NewSendService(SendDependencies{})

	result, err := service.Send(context.Background(), SendCommand{})
	if !errors.Is(err, ErrSendDependenciesMissing) {
		t.Fatalf("Send() error = %v, want ErrSendDependenciesMissing", err)
	}
	if result != (SendResult{}) {
		t.Fatalf("Send() result = %+v, want zero result", result)
	}
}
