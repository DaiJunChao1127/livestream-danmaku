package ws

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"sync"
	"time"

	"github.com/DaiJunChao1127/livestream-danmaku/internal/infra"
	"github.com/DaiJunChao1127/livestream-danmaku/internal/model"
	"github.com/IBM/sarama"
	"github.com/redis/go-redis/v9"
)

type BroadcastJob struct {
	Client  []*Client
	Payload []byte
}

type Manager struct {
	SeverID string

	Register chan *Client

	Unregister chan *Client

	Broadcast chan *model.WsPacket

	// Rooms: Map of (RoomID -> Set of Clients)
	Rooms map[string]map[*Client]struct{}

	// Store cancel functions to stop Redis subscriptions
	cancelSub map[string]context.CancelFunc

	// // Protects the Rooms maps
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
}
