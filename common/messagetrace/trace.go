// Package messagetrace provides opt-in, bounded, in-process timing for local
// diagnostics. It does not change payloads, retry rules, offsets or ACKs.
package messagetrace

import (
	"bufio"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
	"sync"
	"time"

	event "infinitechatgo/common/model/event"
)

const maxRecords = 20000

type Record struct {
	MessageID       int64            `json:"messageId,string"`
	SenderID        int64            `json:"senderId,string"`
	ClientMessageID string           `json:"clientMessageId"`
	OriginUnixMilli int64            `json:"originUnixMilli"`
	Times           map[string]int64 `json:"tNs"`
	flushed         bool
}

type recorder struct {
	mu       sync.Mutex
	origin   time.Time
	observer int64
	rows     map[string]*Record
	file     *os.File
	writer   *bufio.Writer
}

var active = fromEnv()

func fromEnv() *recorder {
	path := os.Getenv("IM_MESSAGE_TRACE_FILE")
	if path == "" {
		return nil
	}
	observer, err := strconv.ParseInt(os.Getenv("IM_MESSAGE_TRACE_USER_ID"), 10, 64)
	if err != nil || observer <= 0 {
		log.Print("message tracing disabled: a positive observer user ID is required")
		return nil
	}
	r, err := openRecorder(path, observer)
	if err != nil {
		log.Printf("message tracing disabled: %v", err)
		return nil
	}
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			if err := r.flush(); err != nil {
				log.Printf("message trace flush failed: %v", err)
			}
		}
	}()
	return r
}

func openRecorder(path string, observer int64) (*recorder, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		return nil, err
	}
	return &recorder{origin: time.Now(), observer: observer, rows: make(map[string]*Record), file: file, writer: bufio.NewWriterSize(file, 64<<10)}, nil
}

func Enabled() bool { return active != nil }

// Stamp uses one process's monotonic clock. Exported durations do not depend
// on synchronizing Kafka/container/Node clocks.
func Stamp() int64 {
	if active == nil {
		return 0
	}
	return time.Since(active.origin).Nanoseconds()
}

func Intent(senderID int64, clientID, stage string) {
	if active == nil {
		return
	}
	active.mark(event.ChatMessage{SenderID: senderID, ClientMessageID: clientID}, stage, Stamp())
}

func Message(message event.ChatMessage, stage string) {
	MessageAt(message, stage, Stamp())
}

func MessageAt(message event.ChatMessage, stage string, at int64) {
	if active == nil {
		return
	}
	active.mark(message, stage, at)
}

// Event is only called for the Push consumer. It decodes identifiers only,
// never records message content, authentication tokens or Kafka payloads.
func Event(payload []byte, stage string, at int64) {
	if active == nil {
		return
	}
	message, ok := identifiers(payload, false)
	if ok {
		active.mark(message, stage, at)
	}
}

func Outgoing(userID int64, payload []byte, stage string) {
	if active == nil || userID != active.observer {
		return
	}
	at := Stamp()
	message, ok := identifiers(payload, true)
	if ok {
		active.mark(message, stage, at)
	}
}

func identifiers(payload []byte, websocket bool) (event.ChatMessage, bool) {
	var envelope struct {
		EventType string `json:"eventType"`
		Message   struct {
			MessageID       int64  `json:"messageId,string"`
			SenderID        int64  `json:"senderId,string"`
			ClientMessageID string `json:"clientMessageId"`
		} `json:"message"`
	}
	if err := json.Unmarshal(payload, &envelope); err != nil || (websocket && envelope.EventType != "message.new") {
		return event.ChatMessage{}, false
	}
	m := event.ChatMessage{MessageID: envelope.Message.MessageID, SenderID: envelope.Message.SenderID, ClientMessageID: envelope.Message.ClientMessageID}
	return m, m.MessageID > 0 && m.SenderID > 0 && m.ClientMessageID != ""
}

func (r *recorder) mark(message event.ChatMessage, stage string, at int64) {
	if message.SenderID <= 0 || message.ClientMessageID == "" {
		return
	}
	key := fmt.Sprintf("%d:%s", message.SenderID, message.ClientMessageID)
	r.mu.Lock()
	defer r.mu.Unlock()
	row := r.rows[key]
	if row == nil {
		if len(r.rows) >= maxRecords {
			return
		}
		row = &Record{SenderID: message.SenderID, ClientMessageID: message.ClientMessageID, OriginUnixMilli: r.origin.UnixMilli(), Times: make(map[string]int64)}
		r.rows[key] = row
	}
	if row.flushed {
		return
	}
	if message.MessageID > 0 {
		row.MessageID = message.MessageID
	}
	// First starts and last ends also expose retry spans if a diagnostic run
	// encounters retries. The measurement report must flag those separately.
	if _, exists := row.Times[stage]; !exists || len(stage) >= 4 && stage[len(stage)-4:] == "_end" {
		row.Times[stage] = at
	}
}

func (r *recorder) flush() error {
	var completed []Record
	r.mu.Lock()
	for _, row := range r.rows {
		if row.flushed || row.Times["kafka_write_end"] == 0 || row.Times["push_commit_end"] == 0 ||
			row.Times["ws_write_end"] == 0 || row.Times["ack_queued"] == 0 || row.Times["committed"] == 0 {
			continue
		}
		copied := *row
		copied.Times = make(map[string]int64, len(row.Times))
		for key, value := range row.Times {
			copied.Times[key] = value
		}
		row.flushed = true
		completed = append(completed, copied)
	}
	r.mu.Unlock()
	for _, row := range completed {
		if err := json.NewEncoder(r.writer).Encode(row); err != nil {
			return err
		}
	}
	return r.writer.Flush()
}
