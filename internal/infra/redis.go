package infra

import (
	"context"
	"fmt"
	"strconv"
	"time"

	"github.com/DaiJunChao1127/livestream-danmaku/internal/logger"
	"github.com/redis/go-redis/v9"
	"go.uber.org/zap"
)

const (
	RedisAddr           = "127.0.0.1:6379"
	KeyRoomPubSub       = "room:%s:pubsub" // Channel for Redis Pub/Sub
	KeyRoomLikes        = "room:%s:likes"  // Counter for likes
	RedisCancelTimeout  = 2 * time.Second
	KeyRoomOnline       = "room:%s:online:*"  // Counter for online users of a room
	KeyRoomServerOnline = "room:%s:online:%s" // Counter for online users of a server
	KeyServerOfRoom     = "room:%s:servers"   // server lists for a specific room
)

func InitRedisClient() *redis.Client {
	rdb := redis.NewClient(&redis.Options{
		Addr:     RedisAddr,
		Password: "", // no password set
		DB:       0,  // use default DB
	})
	return rdb
}

func PublishToRoom(rdb *redis.Client, roomID string, payload []byte) {
	ctx, cancel := context.WithTimeout(context.Background(), RedisCancelTimeout)
	defer cancel()

	channel := fmt.Sprintf(KeyRoomPubSub, roomID) // Redis Pub/Sub的channel名

	// PubSub会长时间持有连接池的一个网络连接
	err := rdb.Publish(ctx, channel, payload).Err()

	if err != nil {
		logger.Log.Error("[REDIS INFRA] Publish failed",
			zap.String("room", roomID),
			zap.Error(err),
		)
	}
}

// 在Redis里给某个房间的点赞总数做原子累加
func IncrRoomLike(rdb *redis.Client, roomID string, count uint64) {
	ctx, cancel := context.WithTimeout(context.Background(), RedisCancelTimeout) // Context上下文 用于控制超时或者传递一些元数据 也可以监控go-redis性能
	defer cancel()

	key := fmt.Sprintf(KeyRoomLikes, roomID)

	err := rdb.IncrBy(ctx, key, int64(count)).Err()
	if err != nil {
		logger.Log.Error("[REDIS INFRA] Increment likes failed",
			zap.String("room", roomID),
			zap.Error(err),
		)
	}
}

// 把当前server在某个房间里的在线人数上报到Redis 并且记录这个房间有哪些server在上报
func UpdateServerOnline(rdb *redis.Client, roomID string, serverID string, count int, ttl time.Duration) {
	ctx := context.Background()
	countKey := fmt.Sprintf(KeyRoomServerOnline, roomID, serverID) // 某房间某实例在线人数
	registerKey := fmt.Sprintf(KeyServerOfRoom, roomID)            // 某房间有哪些实例在上报

	pipe := rdb.Pipeline()
	pipe.Set(ctx, countKey, count, ttl)   // 上报当前实例在线人数并设置TTL作为心跳 若实例失联未续期则该在线记录会自动过期删除
	pipe.SAdd(ctx, registerKey, serverID) // 往Redis的Set registryKey里加入一个成员serverID

	_, err := pipe.Exec(ctx)
	if err != nil {
		logger.Log.Error("[REDIS INFRA] Failed to report server stats", zap.Error(err))
	}
}

// 返回某个房间的所有在线人数
func GetTotalOnline(rdb *redis.Client, roomID string) uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), RedisCancelTimeout)
	defer cancel()
	registryKey := fmt.Sprintf(KeyServerOfRoom, roomID)
	serverIDs, err := rdb.SMembers(ctx, registryKey).Result()
	if err != nil || len(serverIDs) == 0 {
		return 0
	}

	keys := make([]string, len(serverIDs)) // 创建切片
	for i, severID := range serverIDs {
		keys[i] = fmt.Sprintf(KeyRoomServerOnline, roomID, severID)
	}

	values, err := rdb.MGet(ctx, keys...).Result() // keys...是把[]string展开成可变参
	if err != nil {
		logger.Log.Error("[REDIS INFRA] Failed to scan online keys", zap.Error(err))
		return 0
	}

	var total uint64 = 0
	for i, val := range values {
		if val == nil {
			// This server's key expired -> Server might have crashed
			// Clean up the registry set asynchronously
			go rdb.SRem(ctx, registryKey, serverIDs[i]) // 启动一个goroutine异步执行
			continue
		}

		if s, ok := val.(string); ok { // 类型断言 redis返回字符串就继续往下解析
			if i, err := strconv.ParseUint(s, 10, 64); err == nil {
				total += i
			}
		}
	}

	return total
}

func GetRoomLikes(rdb *redis.Client, roomID string) uint64 {
	ctx, cancel := context.WithTimeout(context.Background(), RedisCancelTimeout)
	defer cancel()
	key := fmt.Sprintf(KeyRoomLikes, roomID)

	val, err := rdb.Get(ctx, key).Result() // 线程安全由 Redis 服务端保证
	if err != nil {                        // 失败了
		if err == redis.Nil { // key不存在
			return 0
		}
		logger.Log.Error("[REDIS INFRA] Get likes failed", zap.Error(err))
		return 0
	}

	count, _ := strconv.ParseUint(val, 10, 64)
	return count
}
