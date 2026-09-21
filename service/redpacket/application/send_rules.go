package application

import (
	"errors"
	"fmt"
	"strings"
	"unicode/utf8"
)

const (
	SessionTypeSingle int64 = 0
	SessionTypeGroup  int64 = 1

	RedPacketTypeOrdinary int64 = 0
	RedPacketTypeLucky    int64 = 1

	maxClientMessageIDLength = 64
	maxWrapperTextLength     = 50
	maxRedPacketAmount       = 20_000
	maxGroupPacketCount      = 100
)

var (
	ErrShareAllocationNotImplemented = errors.New("red packet share allocation is not implemented")
)

type SendValidationError struct {
	Field  string
	Reason string
}

func (e SendValidationError) Error() string {
	return fmt.Sprintf("invalid %s: %s", e.Field, e.Reason)
}

func invalidSendField(field, reason string) error {
	return SendValidationError{
		Field:  field,
		Reason: reason,
	}
}

// ValidateSendCommand checks the frozen product rules before IDs are generated
// or any infrastructure is called. It must distinguish a permanent parameter
// rejection from a retryable infrastructure failure.
func ValidateSendCommand(command SendCommand) error {
	if command.SenderID <= 0 {
		return invalidSendField("senderID", "must be positive")
	}

	trimmedMessageID := strings.TrimSpace(command.ClientMessageID)
	if trimmedMessageID == "" || trimmedMessageID != command.ClientMessageID {
		return invalidSendField(
			"clientMessageID",
			"must be non-empty and contain no surrounding whitespace",
		)
	}
	if utf8.RuneCountInString(command.ClientMessageID) > maxClientMessageIDLength {
		return invalidSendField("clientMessageID", "must not exceed 64 characters")
	}

	if command.SessionID <= 0 {
		return invalidSendField("sessionID", "must be positive")
	}
	if command.SessionType != SessionTypeSingle && command.SessionType != SessionTypeGroup {
		return invalidSendField("sessionType", "must be 0 (single) or 1 (group)")
	}
	if command.RedPacketType != RedPacketTypeOrdinary &&
		command.RedPacketType != RedPacketTypeLucky {
		return invalidSendField("redPacketType", "must be 0 (ordinary) or 1 (lucky)")
	}

	if command.TotalAmount < 1 || command.TotalAmount > maxRedPacketAmount {
		return invalidSendField("totalAmount", "must be between 1 and 20000 cents")
	}
	if command.TotalCount < 1 {
		return invalidSendField("totalCount", "must be positive")
	}
	if command.TotalAmount < command.TotalCount {
		return invalidSendField("totalAmount", "must leave at least one cent per share")
	}
	if utf8.RuneCountInString(command.WrapperText) > maxWrapperTextLength {
		return invalidSendField("wrapperText", "must not exceed 50 characters")
	}

	switch command.SessionType {
	case SessionTypeSingle:
		if command.ReceiverID <= 0 {
			return invalidSendField("receiverID", "is required for a single chat")
		}
		if command.ReceiverID == command.SenderID {
			return invalidSendField("receiverID", "must differ from senderID")
		}
		if command.TotalCount != 1 {
			return invalidSendField("totalCount", "must be 1 for a single chat")
		}
	case SessionTypeGroup:
		if command.ReceiverID != 0 {
			return invalidSendField("receiverID", "must be omitted for a group chat")
		}
		if command.TotalCount > maxGroupPacketCount {
			return invalidSendField("totalCount", "must not exceed 100 for a group chat")
		}
	}

	if command.RedPacketType == RedPacketTypeOrdinary &&
		command.TotalAmount%command.TotalCount != 0 {
		return invalidSendField("totalAmount", "must divide evenly by totalCount")
	}

	return nil
}

// AllocateShareAmounts returns exactly count positive amounts whose sum equals
// totalAmount. Ordinary packets must divide evenly; lucky packets must keep at
// least one cent for every remaining share and must terminate for 101/100.
func AllocateShareAmounts(
	totalAmount int64,
	count int64,
	redPacketType int64,
) ([]int64, error) {
	// TODO(user): implement the ordinary and double-mean allocation algorithms.
	return nil, ErrShareAllocationNotImplemented
}
