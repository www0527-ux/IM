package websocket

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	ws "github.com/gorilla/websocket"
)

func TestSlowBusinessRequestDoesNotBlockHeartbeatOrReorderRequests(t *testing.T) {
	started := make(chan struct{})
	unblock := make(chan struct{})
	processed := make(chan string, 3)
	serverDone := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := (&ws.Upgrader{}).Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer close(serverDone)
		client := newClient(1, "test", conn)
		client.readLoop(func(_ *Client, message []byte) {
			if string(message) == "1" {
				close(started)
				<-unblock
			}
			processed <- string(message)
		})
	}))
	defer server.Close()
	conn, _, err := ws.DefaultDialer.Dial("ws"+strings.TrimPrefix(server.URL, "http"), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	released := false
	defer func() {
		if !released {
			close(unblock)
		}
	}()
	if err := conn.WriteMessage(ws.TextMessage, []byte("1")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("handler did not start")
	}
	for _, message := range []string{"2", "3"} {
		if err := conn.WriteMessage(ws.TextMessage, []byte(message)); err != nil {
			t.Fatal(err)
		}
	}
	gotPong := make(chan struct{})
	conn.SetPongHandler(func(string) error { close(gotPong); return conn.SetReadDeadline(time.Now()) })
	if err := conn.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.WriteControl(ws.PingMessage, []byte("heartbeat"), time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	// ReadMessage processes control frames. It returns on the deadline set by
	// the Pong handler, without releasing the deliberately blocked request.
	_, _, _ = conn.ReadMessage()
	select {
	case <-gotPong:
	default:
		t.Fatal("heartbeat blocked behind business work")
	}
	select {
	case v := <-processed:
		t.Fatalf("request ran before first completed: %s", v)
	default:
	}
	close(unblock)
	released = true
	for _, want := range []string{"1", "2", "3"} {
		select {
		case got := <-processed:
			if got != want {
				t.Fatalf("want %s got %s", want, got)
			}
		case <-time.After(2 * time.Second):
			t.Fatal("request processing stalled")
		}
	}
	_ = conn.Close()
	select {
	case <-serverDone:
	case <-time.After(2 * time.Second):
		t.Fatal("reader did not stop")
	}
}
