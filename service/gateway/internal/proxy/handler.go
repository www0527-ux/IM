package proxy

import (
	"fmt"
	"log"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"strings"
	"time"

	"infinitechatgo/service/gateway/internal/config"
)

type handler struct {
	userAPI      *httputil.ReverseProxy
	offlineAPI   *httputil.ReverseProxy
	redPacketAPI *httputil.ReverseProxy
	realtime     *httputil.ReverseProxy
	staticDir    string
}

// NewHandler keeps the original path, query, method, body, and Authorization
// header. Each destination service continues to authenticate its own requests.
func NewHandler(c config.Config) (http.Handler, error) {
	userAPI, err := newReverseProxy("user-api", c.Upstreams.UserAPI)
	if err != nil {
		return nil, err
	}
	offlineAPI, err := newReverseProxy("offline-api", c.Upstreams.OfflineAPI)
	if err != nil {
		return nil, err
	}
	redPacketAPI, err := newReverseProxy("redpacket-api", c.Upstreams.RedPacketAPI)
	if err != nil {
		return nil, err
	}
	realtime, err := newReverseProxy("realtime", c.Upstreams.Realtime)
	if err != nil {
		return nil, err
	}

	staticDir := ""
	if c.StaticDir != "" {
		staticDir, err = filepath.Abs(c.StaticDir)
		if err != nil {
			return nil, fmt.Errorf("resolve static directory: %w", err)
		}
		if info, statErr := os.Stat(filepath.Join(staticDir, "index.html")); statErr != nil || info.IsDir() {
			return nil, fmt.Errorf("static directory %q must contain index.html", staticDir)
		}
	}

	return &handler{
		userAPI:      userAPI,
		offlineAPI:   offlineAPI,
		redPacketAPI: redPacketAPI,
		realtime:     realtime,
		staticDir:    staticDir,
	}, nil
}

func newReverseProxy(name, address string) (*httputil.ReverseProxy, error) {
	target, err := url.Parse(address)
	if err != nil || target.Host == "" || (target.Scheme != "http" && target.Scheme != "https") ||
		(target.Path != "" && target.Path != "/") || target.RawQuery != "" || target.User != nil {
		return nil, fmt.Errorf("%s upstream must be an HTTP(S) origin: %q", name, address)
	}

	p := httputil.NewSingleHostReverseProxy(target)
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.ResponseHeaderTimeout = 30 * time.Second // Also bounds the WebSocket handshake.
	p.Transport = transport
	p.ErrorHandler = func(w http.ResponseWriter, r *http.Request, err error) {
		log.Printf("gateway upstream %s unavailable for %s: %T", name, r.URL.Path, err)
		w.Header().Set("Content-Type", "application/json; charset=utf-8")
		w.WriteHeader(http.StatusBadGateway)
		_, _ = w.Write([]byte(`{"code":502,"message":"上游服务不可用"}`))
	}
	return p, nil
}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch {
	case hasPathPrefix(r.URL.Path, "/api/user"):
		h.userAPI.ServeHTTP(w, r)
	case hasPathPrefix(r.URL.Path, "/api/offline"):
		h.offlineAPI.ServeHTTP(w, r)
	case hasPathPrefix(r.URL.Path, "/api/redpacket"):
		h.redPacketAPI.ServeHTTP(w, r)
	case r.URL.Path == "/ws/chat":
		h.realtime.ServeHTTP(w, r)
	case hasPathPrefix(r.URL.Path, "/api") || hasPathPrefix(r.URL.Path, "/ws"):
		http.NotFound(w, r)
	default:
		h.serveStatic(w, r)
	}
}

func hasPathPrefix(requestPath, prefix string) bool {
	return requestPath == prefix || strings.HasPrefix(requestPath, prefix+"/")
}

func (h *handler) serveStatic(w http.ResponseWriter, r *http.Request) {
	if h.staticDir == "" {
		http.NotFound(w, r)
		return
	}
	if r.Method != http.MethodGet && r.Method != http.MethodHead {
		w.WriteHeader(http.StatusMethodNotAllowed)
		return
	}

	fileName := strings.TrimPrefix(path.Clean("/"+r.URL.Path), "/")
	if fileName == "" {
		fileName = "index.html"
	}
	if strings.ContainsAny(fileName, `:\`) {
		http.NotFound(w, r)
		return
	}
	filePath := filepath.Join(h.staticDir, filepath.FromSlash(fileName))
	relative, err := filepath.Rel(h.staticDir, filePath)
	if err != nil || relative == ".." || strings.HasPrefix(relative, ".."+string(os.PathSeparator)) {
		http.NotFound(w, r)
		return
	}
	info, err := os.Stat(filePath)
	if err != nil || info.IsDir() {
		http.NotFound(w, r)
		return
	}
	http.ServeFile(w, r, filePath)
}
