package messagetrace

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	event "infinitechatgo/common/model/event"
)

func TestFlushWaitsForAllTerminalStages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "trace.jsonl")
	r, err := openRecorder(path, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer r.file.Close()
	m := event.ChatMessage{MessageID: 3, SenderID: 2, ClientMessageID: "request"}
	for _, stage := range []string{"committed", "ack_queued", "kafka_write_end", "ws_write_end"} {
		r.mark(m, stage, 1)
	}
	if err := r.flush(); err != nil {
		t.Fatal(err)
	}
	data, _ := os.ReadFile(path)
	if len(data) != 0 {
		t.Fatal("exported before offset commit completed")
	}
	r.mark(m, "push_commit_end", 2)
	if err := r.flush(); err != nil {
		t.Fatal(err)
	}
	data, _ = os.ReadFile(path)
	var row Record
	if err := json.Unmarshal(data, &row); err != nil {
		t.Fatal(err)
	}
	if row.MessageID != 3 || row.Times["push_commit_end"] != 2 {
		t.Fatalf("unexpected row: %+v", row)
	}
	if err := r.flush(); err != nil {
		t.Fatal(err)
	}
	second, _ := os.ReadFile(path)
	if string(second) != string(data) {
		t.Fatal("row exported twice")
	}
}

func TestOutgoingFiltersObserverAndAcknowledgments(t *testing.T) {
	r, err := openRecorder(filepath.Join(t.TempDir(), "trace.jsonl"), 100)
	if err != nil {
		t.Fatal(err)
	}
	defer r.file.Close()
	previous := active
	active = r
	defer func() { active = previous }()
	payload := []byte(`{"eventType":"message.new","message":{"messageId":"3","senderId":"2","clientMessageId":"request","body":{"content":"private-content"}}}`)
	Outgoing(99, payload, "ws_write_end")
	Outgoing(100, []byte(`{"eventType":"message.ack","messageId":"3"}`), "ws_write_end")
	if len(r.rows) != 0 {
		t.Fatal("recorded another user or ACK")
	}
	Outgoing(100, payload, "ws_write_end")
	if len(r.rows) != 1 {
		t.Fatal("observer message missing")
	}
	data, err := json.Marshal(r.rows)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "private-content") {
		t.Fatal("message body leaked")
	}
	if _, err := openRecorder(r.file.Name(), 100); err == nil {
		t.Fatal("existing trace file overwritten")
	}
}
