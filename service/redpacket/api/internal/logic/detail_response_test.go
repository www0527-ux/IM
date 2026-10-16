package logic

import (
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"infinitechatgo/service/redpacket/application"
)

func TestDetailReturnsClaimsWithoutShareIDsAndSentListStaysSummary(t *testing.T) {
	claimedAt := time.Date(2026, 9, 29, 10, 0, 0, 0, time.UTC)
	detail := application.PacketDetail{
		RedPacketID: 1, TotalCount: 2,
		Receives: []application.PacketReceive{{ReceiverID: 42, Amount: 100, ReceivedAt: claimedAt}},
	}
	encoded, err := json.Marshal(detailResponse(detail))
	if err != nil {
		t.Fatal(err)
	}
	value := string(encoded)
	if !strings.Contains(value, `"receiverId":"42"`) ||
		!strings.Contains(value, `"amount":100`) ||
		!strings.Contains(value, `"receivedAt":`+strconv.FormatInt(claimedAt.UnixMilli(), 10)) ||
		strings.Contains(value, "shareId") {
		t.Fatalf("unexpected detail JSON: %s", value)
	}
	// An unclaimed packet still gives clients an array they can render directly.
	empty, err := json.Marshal(detailResponse(application.PacketDetail{}))
	if err != nil || !strings.Contains(string(empty), `"receives":[]`) {
		t.Fatalf("empty detail JSON = %s, error = %v", empty, err)
	}
	summary, err := json.Marshal(sentPacketSummary(detail))
	if err != nil || strings.Contains(string(summary), "receives") {
		t.Fatalf("sent summary JSON = %s, error = %v", summary, err)
	}
}
