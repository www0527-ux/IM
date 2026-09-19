package websocket

import "sync"

// Manager keeps one active WebSocket per login session while allowing one
// user to be online from multiple sessions/devices.
type Manager struct {
	mu      sync.RWMutex
	clients map[int64]map[string]*Client
}

func NewManager() *Manager {
	return &Manager{
		clients: make(map[int64]map[string]*Client),
	}
}

func (m *Manager) add(client *Client) {
	m.mu.Lock()
	userClients := m.clients[client.userID]
	if userClients == nil {
		userClients = make(map[string]*Client)
		m.clients[client.userID] = userClients
	}
	previous := userClients[client.loginSessionID]
	userClients[client.loginSessionID] = client
	m.mu.Unlock()

	// Closing the previous connection outside the lock avoids blocking manager
	// operations on network cleanup. Its deferred remove cannot delete the new
	// connection because remove checks pointer identity.
	if previous != nil && previous != client {
		previous.close()
	}
}

func (m *Manager) remove(client *Client) {
	m.mu.Lock()
	userClients := m.clients[client.userID]
	if userClients != nil {
		if current := userClients[client.loginSessionID]; current == client {
			delete(userClients, client.loginSessionID)
		}
		if len(userClients) == 0 {
			delete(m.clients, client.userID)
		}
	}
	m.mu.Unlock()

	client.close()
}

// SendToUser queues the payload for every active login session of a user.
// Its result is the number of local connection queues that accepted it, not a
// guarantee that clients received or processed the payload.
func (m *Manager) SendToUser(userID int64, payload []byte) int {
	m.mu.RLock()
	userClients := m.clients[userID]
	clients := make([]*Client, 0, len(userClients))
	for _, client := range userClients {
		clients = append(clients, client)
	}
	m.mu.RUnlock()

	queued := 0
	for _, client := range clients {
		if client.sendMessage(payload) {
			queued++
		}
	}
	return queued
}
