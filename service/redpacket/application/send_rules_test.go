package application

import (
	"errors"
	"strings"
	"testing"
)

func TestValidateSendCommandAcceptsValidCommands(t *testing.T) {
	tests := []struct {
		name    string
		command SendCommand
	}{
		{
			name: "single",
			command: SendCommand{
				SenderID:        1,
				ClientMessageID: "single-message-1",
				SessionID:       10,
				SessionType:     SessionTypeSingle,
				ReceiverID:      2,
				RedPacketType:   RedPacketTypeOrdinary,
				TotalAmount:     100,
				TotalCount:      1,
			},
		},
		{
			name: "group lucky",
			command: SendCommand{
				SenderID:        1,
				ClientMessageID: "group-message-1",
				SessionID:       20,
				SessionType:     SessionTypeGroup,
				RedPacketType:   RedPacketTypeLucky,
				WrapperText:     "恭喜发财",
				TotalAmount:     101,
				TotalCount:      100,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if err := ValidateSendCommand(test.command); err != nil {
				t.Fatalf("ValidateSendCommand() error = %v", err)
			}
		})
	}
}

func TestValidateSendCommandRejectsInvalidFields(t *testing.T) {
	valid := SendCommand{
		SenderID:        1,
		ClientMessageID: "message-1",
		SessionID:       20,
		SessionType:     SessionTypeGroup,
		RedPacketType:   RedPacketTypeLucky,
		TotalAmount:     100,
		TotalCount:      10,
	}

	tests := []struct {
		name      string
		mutate    func(*SendCommand)
		wantField string
	}{
		{name: "sender", mutate: func(c *SendCommand) { c.SenderID = 0 }, wantField: "senderID"},
		{name: "blank message id", mutate: func(c *SendCommand) { c.ClientMessageID = "   " }, wantField: "clientMessageID"},
		{name: "padded message id", mutate: func(c *SendCommand) { c.ClientMessageID = " message " }, wantField: "clientMessageID"},
		{name: "long message id", mutate: func(c *SendCommand) { c.ClientMessageID = strings.Repeat("a", 65) }, wantField: "clientMessageID"},
		{name: "session id", mutate: func(c *SendCommand) { c.SessionID = 0 }, wantField: "sessionID"},
		{name: "session type", mutate: func(c *SendCommand) { c.SessionType = 2 }, wantField: "sessionType"},
		{name: "packet type", mutate: func(c *SendCommand) { c.RedPacketType = 2 }, wantField: "redPacketType"},
		{name: "zero amount", mutate: func(c *SendCommand) { c.TotalAmount = 0 }, wantField: "totalAmount"},
		{name: "amount over limit", mutate: func(c *SendCommand) { c.TotalAmount = 20_001 }, wantField: "totalAmount"},
		{name: "zero count", mutate: func(c *SendCommand) { c.TotalCount = 0 }, wantField: "totalCount"},
		{name: "less than one cent each", mutate: func(c *SendCommand) { c.TotalAmount = 9 }, wantField: "totalAmount"},
		{name: "long wrapper", mutate: func(c *SendCommand) { c.WrapperText = strings.Repeat("福", 51) }, wantField: "wrapperText"},
		{name: "group receiver", mutate: func(c *SendCommand) { c.ReceiverID = 2 }, wantField: "receiverID"},
		{name: "group count", mutate: func(c *SendCommand) { c.TotalAmount = 101; c.TotalCount = 101 }, wantField: "totalCount"},
		{name: "ordinary remainder", mutate: func(c *SendCommand) { c.RedPacketType = RedPacketTypeOrdinary; c.TotalAmount = 101 }, wantField: "totalAmount"},
		{name: "single receiver missing", mutate: func(c *SendCommand) { c.SessionType = SessionTypeSingle; c.TotalCount = 1 }, wantField: "receiverID"},
		{name: "single self receiver", mutate: func(c *SendCommand) { c.SessionType = SessionTypeSingle; c.TotalCount = 1; c.ReceiverID = c.SenderID }, wantField: "receiverID"},
		{name: "single count", mutate: func(c *SendCommand) { c.SessionType = SessionTypeSingle; c.ReceiverID = 2 }, wantField: "totalCount"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			command := valid
			test.mutate(&command)

			err := ValidateSendCommand(command)
			var validationErr SendValidationError
			if !errors.As(err, &validationErr) {
				t.Fatalf("ValidateSendCommand() error = %v, want SendValidationError", err)
			}
			if validationErr.Field != test.wantField {
				t.Fatalf("validation field = %q, want %q", validationErr.Field, test.wantField)
			}
		})
	}
}

func TestAllocateShareAmountsPreservesMoneyAndMinimumShare(t *testing.T) {
	for _, test := range []struct {
		name  string
		total int64
		count int64
		kind  int64
	}{
		{name: "ordinary", total: 100, count: 10, kind: RedPacketTypeOrdinary},
		{name: "lucky minimum edge", total: 101, count: 100, kind: RedPacketTypeLucky},
		{name: "lucky maximum", total: 20_000, count: 100, kind: RedPacketTypeLucky},
	} {
		t.Run(test.name, func(t *testing.T) {
			amounts, err := AllocateShareAmounts(test.total, test.count, test.kind)
			if err != nil {
				t.Fatalf("AllocateShareAmounts() error = %v", err)
			}
			if int64(len(amounts)) != test.count {
				t.Fatalf("share count = %d, want %d", len(amounts), test.count)
			}
			var sum int64
			for _, amount := range amounts {
				if amount < 1 || amount > maxRedPacketAmount {
					t.Fatalf("invalid share amount %d", amount)
				}
				if test.kind == RedPacketTypeOrdinary && amount != test.total/test.count {
					t.Fatalf("ordinary share = %d, want %d", amount, test.total/test.count)
				}
				sum += amount
			}
			if sum != test.total {
				t.Fatalf("share sum = %d, want %d", sum, test.total)
			}
		})
	}
}
