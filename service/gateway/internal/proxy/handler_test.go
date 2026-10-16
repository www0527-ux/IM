package proxy

import (
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"infinitechatgo/service/gateway/internal/config"

	"github.com/gorilla/websocket"
)

func testConfig(upstream string) config.Config {
	return config.Config{Upstreams: config.Upstreams{
		UserAPI: upstream, OfflineAPI: upstream,
		RedPacketAPI: upstream, Realtime: upstream,
	}}
}

func TestRoutesKeepRequestAndResponse(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		w.Header().Set("X-Upstream", "received")
		w.WriteHeader(http.StatusCreated)
		_, _ = io.WriteString(w, r.Method+" "+r.URL.RequestURI()+" "+r.Header.Get("Authorization")+" "+string(body))
	}))
	defer upstream.Close()

	h, err := NewHandler(testConfig(upstream.URL))
	if err != nil {
		t.Fatal(err)
	}
	for _, requestPath := range []string{
		"/api/user/loginPassword", "/api/offline/sync", "/api/redpacket/send",
	} {
		req := httptest.NewRequest(http.MethodPost, requestPath+"?page=2", strings.NewReader("hello"))
		req.Header.Set("Authorization", "Bearer example")
		response := httptest.NewRecorder()
		h.ServeHTTP(response, req)
		want := "POST " + requestPath + "?page=2 Bearer example hello"
		if response.Code != http.StatusCreated || response.Body.String() != want || response.Header().Get("X-Upstream") != "received" {
			t.Errorf("%s: status=%d body=%q upstream=%q", requestPath,
				response.Code, response.Body.String(), response.Header().Get("X-Upstream"))
		}
	}

	for _, requestPath := range []string{"/api/users", "/api/missing", "/ws/other"} {
		response := httptest.NewRecorder()
		h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, requestPath, nil))
		if response.Code != http.StatusNotFound {
			t.Errorf("%s: got status %d", requestPath, response.Code)
		}
	}
}

func TestUpstreamFailureReturnsBadGateway(t *testing.T) {
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	c := testConfig(upstream.URL)
	upstream.Close()
	h, err := NewHandler(c)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/user/ping", nil))
	if response.Code != http.StatusBadGateway {
		t.Fatalf("got status %d, want 502", response.Code)
	}
}

func TestWebSocketUpgradePassesThrough(t *testing.T) {
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	upstream := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/ws/chat" || r.URL.Query().Get("access_token") != "sample" {
			http.Error(w, "unexpected websocket request", http.StatusBadRequest)
			return
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		messageType, payload, err := conn.ReadMessage()
		if err == nil {
			_ = conn.WriteMessage(messageType, payload)
		}
	}))
	defer upstream.Close()
	h, err := NewHandler(testConfig(upstream.URL))
	if err != nil {
		t.Fatal(err)
	}
	gateway := httptest.NewServer(h)
	defer gateway.Close()

	conn, response, err := websocket.DefaultDialer.Dial(
		"ws"+strings.TrimPrefix(gateway.URL, "http")+"/ws/chat?access_token=sample", nil)
	if err != nil {
		t.Fatalf("dial websocket: %v, response=%v", err, response)
	}
	defer conn.Close()
	if err := conn.WriteMessage(websocket.TextMessage, []byte("hello")); err != nil {
		t.Fatal(err)
	}
	_, payload, err := conn.ReadMessage()
	if err != nil || string(payload) != "hello" {
		t.Fatalf("read websocket echo: payload=%q error=%v", payload, err)
	}
}

func TestStaticBuildFiles(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "index.html"), []byte("frontend"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "assets"), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "assets", "app.js"), []byte("app"), 0o600); err != nil {
		t.Fatal(err)
	}
	upstream := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	defer upstream.Close()
	c := testConfig(upstream.URL)
	c.StaticDir = dir
	h, err := NewHandler(c)
	if err != nil {
		t.Fatal(err)
	}
	response := httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/", nil))
	if response.Code != http.StatusOK || response.Body.String() != "frontend" {
		t.Fatalf("static page: status=%d body=%q", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/assets/app.js", nil))
	if response.Code != http.StatusOK || response.Body.String() != "app" {
		t.Fatalf("static asset: status=%d body=%q", response.Code, response.Body.String())
	}
	response = httptest.NewRecorder()
	h.ServeHTTP(response, httptest.NewRequest(http.MethodGet, "/api/unknown", nil))
	if response.Code != http.StatusNotFound {
		t.Fatalf("unknown API request served as frontend: status=%d", response.Code)
	}
}
