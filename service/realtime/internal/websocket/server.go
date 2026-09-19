package websocket

import (
	"errors"
	"log"
	"net/http"
	"strings"

	"infinitechatgo/common/auth"
	"infinitechatgo/service/realtime/internal/svc"

	ws "github.com/gorilla/websocket"
)

const wsPath = "/ws/chat"

var upgrader = ws.Upgrader{
	ReadBufferSize:  1024,
	WriteBufferSize: 1024,
	CheckOrigin: func(r *http.Request) bool {
		return true
	},
}

func StartServer(
	addr string,
	manager *Manager,
	svcCtx *svc.ServiceContext,
) error {
	mux := http.NewServeMux()
	mux.HandleFunc(
		wsPath,
		func(w http.ResponseWriter, r *http.Request) {
			handleConnection(
				w,
				r,
				manager,
				svcCtx,
			)
		},
	)

	log.Printf(
		"WebSocket server listening on %s%s",
		addr,
		wsPath,
	)

	return http.ListenAndServe(addr, mux)
}

func handleConnection(
	w http.ResponseWriter,
	r *http.Request,
	manager *Manager,
	svcCtx *svc.ServiceContext,
) {
	accessToken, ok := bearerToken(
		r.Header.Get("Authorization"),
	)
	// Browsers cannot attach an Authorization header when constructing a
	// WebSocket. Keep the header as the preferred production path and allow a
	// query parameter for the local web client used during development.
	if !ok {
		accessToken = strings.TrimSpace(
			r.URL.Query().Get("access_token"),
		)
		ok = accessToken != ""
	}
	if !ok {
		http.Error(
			w,
			"unauthorized",
			http.StatusUnauthorized,
		)
		return
	}

	identity, err :=
		svcCtx.Authenticator.AuthenticateAccessToken(
			r.Context(),
			accessToken,
		)

	if errors.Is(err, auth.ErrInvalidAccessToken) {
		http.Error(
			w,
			"unauthorized",
			http.StatusUnauthorized,
		)
		return
	}

	if err != nil {
		log.Printf(
			"authenticate websocket failed: %v",
			err,
		)
		http.Error(
			w,
			"internal server error",
			http.StatusInternalServerError,
		)
		return
	}

	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf(
			"WebSocket upgrade failed: %v",
			err,
		)
		return
	}

	client := newClient(
		identity.UserID,
		identity.LoginSessionID,
		conn,
	)

	manager.add(client)
	defer manager.remove(client)

	log.Printf(
		"WebSocket connected: userId=%d, loginSessionId=%s, remote=%s",
		identity.UserID,
		identity.LoginSessionID,
		r.RemoteAddr,
	)

	go client.writeLoop()

	client.readLoop(
		func(sender *Client, message []byte) {
			handleMessage(
				sender,
				message,
				svcCtx,
			)
		},
	)
}

func bearerToken(header string) (string, bool) {
	fields := strings.Fields(header)

	if len(fields) != 2 ||
		!strings.EqualFold(fields[0], "Bearer") ||
		fields[1] == "" {
		return "", false
	}

	return fields[1], true
}
