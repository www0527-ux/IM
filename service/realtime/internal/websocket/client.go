package websocket

import (
	"log"
	"sync"
	"time"

	ws "github.com/gorilla/websocket"
)

const (
	sendQueueSize = 256

	// 单条WebSocket消息最大64KB。
	maxMessageSize int64 = 64 << 10

	// 超过60秒没有收到Pong，认为连接失效。
	pongWait = 60 * time.Second

	// 每54秒发送一次Ping，必须小于pongWait。
	pingPeriod = pongWait * 9 / 10

	// 单次写操作最多等待10秒。
	writeWait = 10 * time.Second
)

// 一个连接
type Client struct {
	conn      *ws.Conn
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once

	userID         int64
	loginSessionID string
}

func newClient(
	userID int64,
	loginSessionID string,
	conn *ws.Conn,
) *Client {
	return &Client{
		userID:         userID,
		loginSessionID: loginSessionID,
		conn:           conn,
		send:           make(chan []byte, sendQueueSize),
		done:           make(chan struct{}),
	}
}

func (c *Client) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}
func (c *Client) readLoop(
	onMessage func(*Client, []byte),
) {
	defer c.close()
	//限制客户端发送的消息内存大小
	c.conn.SetReadLimit(maxMessageSize)

	//设置第一次读取期限
	if err:=c.conn.SetReadDeadline(
		time.Now().Add(pongWait),
	);err!=nil{
		log.Printf(
			"set initial read deadline failed:%v",
			err,
		)
		return
	}
	//收到pong后，就将读取期限往后面延长了60秒
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(
			time.Now().Add(pongWait),
		)
	})
	for {
		messageType, message, err := c.conn.ReadMessage()
		if err != nil {
			log.Printf("read message failed: %v", err)
			return
		}
		if messageType != ws.TextMessage {
			continue
		}
		log.Printf("received:%s", string(message))
		onMessage(c, message)
	}
}
func (c *Client) sendMessage(message []byte) bool {
	select {
	case <-c.done:
		return false
	default:
	}
	select {
	case c.send <- message:
		return true
	case <-c.done:
		return false
	default:
		log.Printf("send queue is full")
		return false
	}
}

// 使用管道连接此循环，达到流量控制并发消息的功能
func (c *Client) writeLoop() {
	ticker:=time.NewTicker(pingPeriod)
	defer ticker.Stop()
	defer c.close()

	for {
		select {
		case message := <-c.send:
			if err:=c.conn.SetWriteDeadline(
				time.Now().Add(writeWait),
			);err!=nil{
				log.Printf(
					"set write deadline failed:%v",
					err,
				)
				return
			}

			if err := c.conn.WriteMessage(
				ws.TextMessage,
				message,
			); err != nil {
				log.Printf(
					"write message failed: %v",
					err,
				)
				return
			}
		case <-ticker.C:
			if err:=c.conn.SetWriteDeadline(
				time.Now().Add(writeWait),
			);err!=nil{
				log.Printf(
					"set ping deadline failed: %v",
					err,
				)
				return
			}
			if err := c.conn.WriteMessage(
				ws.PingMessage,
				nil,
			); err != nil {
				log.Printf(
					"write ping failed: %v",
					err,
				)
				return
			}
		case <-c.done:
			return
		}
	}
}
