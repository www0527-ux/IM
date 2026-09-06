package websocket

import (
	"sync"
)

// 连接管理器
type Manager struct {
	mu      sync.RWMutex
	clients map[int64]map[*Client]struct{}
}
//初始化连接管理器(userID->Client)
func newManager() *Manager {
	return &Manager{
		clients: make(
			map[int64]map[*Client]struct{},
		),
	}
}

func (m *Manager) add(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()
	userClients := m.clients[client.userID]
	if userClients == nil {
		//创建新的内层map
		userClients = make(map[*Client]struct{})
		m.clients[client.userID] = userClients
	}
	//共享底层，类型为空结构体
	userClients[client] = struct{}{}
}

// 去掉连接
func (m *Manager) remove(client *Client) {
	//写操作使用写锁
	m.mu.Lock()

	userClients := m.clients[client.userID]
	if userClients != nil {
		delete(userClients, client)

		if len(userClients) == 0 {
			delete(m.clients, client.userID)
		}
	}

	m.mu.Unlock()

	client.close()
}

// RW锁确保了多读者时依旧可以持有RLock
func (m *Manager) sendToUser(
	userID int64,
	message []byte,
) int {
	//统一转发给在线的用户
	m.mu.RLock()
	userClients := m.clients[userID]
	clients := make(
		[]*Client,
		0,
		len(userClients),
	)
	for client := range userClients {
		clients = append(clients, client)
	}
	m.mu.RUnlock()
	sendCount := 0
	for _, client := range clients {
		if client.sendMessage(message) {
			sendCount++
		}
	}
	return sendCount
}
