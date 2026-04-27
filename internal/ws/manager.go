package ws

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/DaiJunChao1127/livestream-danmaku/internal/infra"
	"github.com/DaiJunChao1127/livestream-danmaku/internal/logger"
	"github.com/DaiJunChao1127/livestream-danmaku/internal/model"
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

type BroadcastJob struct {
	Clients []*Client // 要发送给的Client
	Payload []byte    // 要发送的数据
}

type Manager struct {
	ServerID string

	Register chan *Client

	Unregister chan *Client

	Broadcast chan *model.WsPacket

	// Rooms: Map of (RoomID -> Set of Clients)
	Rooms map[string]map[*Client]struct{}

	// Store cancel functions to stop Redis subscriptions
	cancelSub map[string]context.CancelFunc

	// Protects the Rooms maps
	mu sync.RWMutex

	clientPool sync.Pool

	broadcastJobChan chan *BroadcastJob

	localLikes map[string]uint64 // Map of (RoomID -> Count of likes)
	likesMu    sync.Mutex

	RedisClient *redis.Client

	KafkaProducer sarama.AsyncProducer
}

const (
	BroadCastInterVal    = 3 * time.Second
	InitClientPoolCap    = 500
	BroadcastChanSize    = 1024
	BroadcastJobChanSize = 1000
	WorkerCount          = 100
)

func NewManager() *Manager {
	// Generate a unique ID for this server instance.
	// In k8s, you might use os.Hostname().
	// Here we use Hostname + Random Int to ensure uniqueness during local restarts.
	hostname, _ := os.Hostname()
	serverID := fmt.Sprintf("%s-%d", hostname, rand.Intn(100000))

	brokers := []string{"127.0.0.1:9092"} // Kafka 集群入口地址清单
	rdb := infra.InitRedisClient()
	producer := infra.InitKafkaProducer(brokers)
	return &Manager{
		ServerID:   serverID,
		Register:   make(chan *Client),
		Unregister: make(chan *Client),
		Broadcast:  make(chan *model.WsPacket, BroadcastChanSize), // Buffer to handle spikes
		localLikes: make(map[string]uint64),
		Rooms:      make(map[string]map[*Client]struct{}),
		cancelSub:  make(map[string]context.CancelFunc),
		clientPool: sync.Pool{
			New: func() interface{} { // 当池里没有可用对象时 如何创建一个新的
				return make([]*Client, 0, InitClientPoolCap)
			},
		},
		// Initialize Job Channel
		broadcastJobChan: make(chan *BroadcastJob, BroadcastJobChanSize),
		RedisClient:      rdb,
		KafkaProducer:    producer,
	}
}

func (m *Manager) safeSend(client *Client, payload []byte) {
	select {
	case client.Send <- payload:
	default:
		// Buffer full, skip this client
		// In a real system, you might want to disconnect the client here.
		logger.Log.Warn("[MANAGER] Client buffer full, dropping message", zap.Uint64("uid", client.UserID))
	}
}

// broadcastWorker consumes jobs from the channel and sends messages to clients.
func (m *Manager) broadcastWorker() {
	// 1. Iterate and Send
	for job := range m.broadcastJobChan {

		for _, client := range job.Clients {
			m.safeSend(client, job.Payload)
		}

		// 2. Return the slice to the Pool HERE
		m.clientPool.Put(job.Clients)
	}
}

func (m *Manager) StartWorkers() {
	for i := 0; i < WorkerCount; i++ {
		go m.broadcastWorker()
	}
}

// Create the main loop for the manager
func (m *Manager) Start() {
	logger.Log.Info("[MANAGER] Multi-room Manager started")
	m.StartWorkers()
	statsTicker := time.NewTicker(BroadCastInterVal)
	defer statsTicker.Stop()

	for {
		select {
		case client := <-m.Register: // 用户进房间
			m.handleRegister(client)
		case client := <-m.Unregister: // 用户退房间
			m.handleUnregister(client)
		case packet := <-m.Broadcast: // 用户发弹幕 点赞 统计
			m.handleBroadcast(packet)
		case <-statsTicker.C: // 每隔固定时间 Ticker会往这个channel里发送一次当前时间
			// Do NOT call m.broadcastStats() directly here.
			// broadcastStats involves Redis I/O and loops through all rooms.
			// If run synchronously, it blocks the Register/Unregister channels,
			// causing a bottleneck (Head-of-Line Blocking).
			go m.broadcastStats() // 定时统计
		}
	}
}

// subscribeToRoom listens to a specific Redis channel for a room
func (m *Manager) subScribeToRoom(ctx context.Context, roomID string) {
	// Redis Pub/Sub channel
	channelName := fmt.Sprintf(infra.KeyRoomPubSub, roomID)
	pubsub := m.RedisClient.Subscribe(ctx, channelName) // 当前server订阅Redis里的某个房间频道(别的server也会订阅)
	defer pubsub.Close()                                // 取消订阅
	ch := pubsub.Channel()

	// 一旦订阅频道就开始持续工作
	// 把从频道收到的消息广播给本server中这个roomID房间里的客户端
	for {
		select {
		// 调用cancel() ctx.Done()这个channel就会收到信号
		case <-ctx.Done():
			return
		case msg, ok := <-ch:
			if !ok { // ch被关闭
				return
			}
			// 广播给当前server本机在这个roomID房间里的客户端
			m.broadcastToLocalClients(roomID, []byte(msg.Payload))
		}
	}
}

// broadcastToLocalClients sends raw bytes to all clients(当前server实例) in a specific room.
func (m *Manager) broadcastToLocalClients(roomID string, payload []byte) {
	m.mu.Lock()
	clients, ok := m.Rooms[roomID] // 当前server实例中 该roomID对应的本地客户端集合
	if !ok {
		m.mu.Unlock()
		return
	}
	targetClients := m.clientPool.Get().([]*Client) // 拿出一个可复用切片
	targetClients = targetClients[:0]               // 将切片长度清零 保留底层数组容量
	for c := range clients {
		targetClients = append(targetClients, c)
	}
	m.mu.Unlock()

	// 构造广播任务 包含本次消息内容和当前server上的目标客户端列表
	job := &BroadcastJob{
		Clients: targetClients,
		Payload: payload,
	}

	select {
	case m.broadcastJobChan <- job:
	default:
		// Failsafe: If workers are too slow, we must drop the job
		// and return the slice to pool immediately to prevent leak
		logger.Log.Warn("[MANAGER] Broadcast job queue full, dropping message", zap.String("room", roomID))
		m.clientPool.Put(targetClients)
	}
}

func (m *Manager) handleRegister(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, ok := m.Rooms[client.RoomID]; !ok {
		// 创建内层map
		m.Rooms[client.RoomID] = make(map[*Client]struct{})

		ctx, cancel := context.WithCancel(context.Background())
		m.cancelSub[client.RoomID] = cancel // 负责让负责订阅的goroutine退出

		// 让当前server订阅某个client.RoomID的Redis Pub/Sub频道
		// 并把收到的消息转发给当前server本机该房间的客户端
		// 启动一个负责订阅的goroutine
		go m.subScribeToRoom(ctx, client.RoomID)

		logger.Log.Info("[MANAGER] New room created on server", zap.String("room", client.RoomID))
	}

	m.Rooms[client.RoomID][client] = struct{}{} // 把当前client加入对应房间的客户端集合(map模拟set)
}

func (m *Manager) handleUnregister(client *Client) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if clients, ok := m.Rooms[client.RoomID]; ok {
		if _, ok := clients[client]; ok {
			delete(clients, client)

			client.Close()

			if len(clients) == 0 {
				delete(m.Rooms, client.RoomID)
				if cancel, exists := m.cancelSub[client.RoomID]; exists {
					cancel() // 取消订阅
					delete(m.cancelSub, client.RoomID)
					logger.Log.Info("[MANAGER] Room subscription stopped", zap.String("room", client.RoomID))
				}
			}
		}
	}
}

func (m *Manager) processDanmaku(roomID string, data []byte) {
	// Async IO to Redis and Kafka

	// 1. Persistence (Kafka)
	infra.PushtoInput(m.KafkaProducer, infra.DanmakuSaveTopic, data)
	// 2. Distribution (Redis Pub/Sub)
	// // This ensures clients on other servers also receive this danmaku.
	infra.PublishToRoom(m.RedisClient, roomID, data)
}

func (m *Manager) handleBroadcast(packet *model.WsPacket) {
	payload, err := json.Marshal(packet) // 把 packet 转成 JSON 字节数据
	if err != nil {
		logger.Log.Error("[MANAGER] Marshal failed", zap.Error(err))
		return
	}

	switch packet.Type {
	case model.TypeDanmaku:
		m.processDanmaku(packet.RoomID, payload)
	case model.ActionLike:
		// that is not gonna happen
	case model.TypeStatus:
		m.broadcastToLocalClients(packet.RoomID, payload)
	default:
		logger.Log.Warn("[MANAGER] Unknown packet type", zap.Int("type", packet.Type))
	}
}

// broadcastStats fetches stats for ALL active rooms and sends updates locally.
func (m *Manager) broadcastStats() {
	m.likesMu.Lock()
	currentBatchLikes := m.localLikes
	m.localLikes = make(map[string]uint64)
	m.likesMu.Unlock()

	m.mu.RLock()
	localStatus := make(map[string]int)
	for roomID, clients := range m.Rooms {
		localStatus[roomID] = len(clients)
	}
	m.mu.RUnlock()

	reportTTL := BroadCastInterVal*2 + time.Second

	for roomID, count := range localStatus {
		// Step1: Report "I am alive and I have X users" to Redis
		infra.UpdateServerOnline(m.RedisClient, roomID, m.ServerID, count, reportTTL)
		if likeDelta, ok := currentBatchLikes[roomID]; ok && likeDelta > 0 {
			go infra.IncrRoomLike(m.RedisClient, roomID, likeDelta)
		}

		// STEP B: Fetch the global total (Sum of all servers)
		totalOnline := infra.GetTotalOnline(m.RedisClient, roomID)
		totalLikes := infra.GetRoomLikes(m.RedisClient, roomID)
		status := model.StatsData{
			Online: totalOnline,
			Likes:  totalLikes,
		}

		logger.Log.Info("[MANAGER] Room Stats",
			zap.String("roomID", roomID),
			zap.Uint64("likes", totalLikes),
			zap.Uint64("online", totalOnline),
		)

		dataBytes, _ := json.Marshal(status)

		// CreatePacket
		packet := &model.WsPacket{
			Type:   model.TypeStatus,
			RoomID: roomID,
			Data:   dataBytes,
		}

		payload, err := json.Marshal(packet)
		if err != nil {
			logger.Log.Error("[MANAGER] Marshal stats packet failed", zap.Error(err))
			continue
		}
		m.broadcastToLocalClients(roomID, payload)
	}
}
