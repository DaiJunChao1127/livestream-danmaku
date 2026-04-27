package infra

import (
	"time"

	"github.com/DaiJunChao1127/livestream-danmaku/internal/logger"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

// Kafka 快速理解（本项目版）
//
// Kafka 是什么？
// - Kafka 是一个分布式消息队列 / 事件日志系统。
// - 生产者（Producer）负责写消息，消费者（Consumer）负责读消息。
// - 主要价值：把“实时链路”和“慢操作（如写库）”解耦。
//
// 核心概念：
// - Broker：Kafka 服务节点（例如 127.0.0.1:9092）。
// - Topic：消息分类/频道（例如 danmaku_save_topic）。
// - Partition：Topic 的分片，用于并行和扩展吞吐。
// - Consumer Group：同组消费者分摊消费，一个分区同一时刻只会被组内一个消费者处理。
// - Offset：消费者在分区中的读取位置（游标）。
//
// 本项目为什么用 Kafka：
// - WebSocket 服务要先保证“弹幕实时广播”。
// - MySQL 写入可能慢，不能阻塞实时链路。
// - 所以先把弹幕写入 Kafka，再由独立 consumer 批量落库。
// - 这样既保证实时性，也提高整体吞吐和稳定性。
//
// 本项目数据流：
// 1) Server 收到用户弹幕。
// 2) Server 把弹幕写入 topic: danmaku_save_topic。
// 3) Consumer 订阅该 topic 并读取消息。
// 4) Consumer 解析后批量写入 MySQL。
//
// 取舍说明：
// - 若“先标记消费成功，再写库”，进程崩溃时可能丢少量消息（性能更高）。
// - 若要更强一致性，需要调整消费确认与落库策略。

const (
	DanmakuSaveTopic    = "danmaku_save_topic" // Topic name for danmaku saving(kafka按topic分流数据 未来可以扩展)
	ProducerChanBufSize = 16384
)

func InitKafkaProducer(brokers []string) sarama.AsyncProducer {
	config := sarama.NewConfig()
	config.ChannelBufferSize = ProducerChanBufSize //Allow more buffering in memory

	// We must wait for the acknowledgment from Kafka to ensure data is safe.
	//   - NoResponse
	//   - WaitForLocal: Leader returns OK after receiving
	//   - WaitForAll: all follower synced
	// For Danmaku, speed > absolute consistency.
	config.Producer.RequiredAcks = sarama.WaitForLocal

	config.Producer.Compression = sarama.CompressionSnappy

	config.Producer.Flush.Messages = 50
	config.Producer.Flush.MaxMessages = 200
	config.Producer.Flush.Frequency = 50 * time.Millisecond
	config.Producer.Flush.Bytes = 8 * 1024 // 8KB

	// config.Producer.Return.Successes = true
	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true

	// Kafka中Topic由多个Partition组成 而每个Partition本质上是按顺序追加消息的日志
	// multiple partitions in one topic of Kafka
	// Use Random partitioner to distribute messages evenly in all partitions
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	producer, err := sarama.NewAsyncProducer(brokers, config)
	if err != nil {
		// In production, we might want to retry or fail gracefully.
		// For now, we panic because without Kafka, persistence fails.
		logger.Log.Panic("[KAFKA INFRA]Failed to start Kafka async producer", zap.Error(err))
	}

	// 起一个后台goroutine持续读取并打日志 异步消费Kafka producer的错误通道
	go func() {
		for err := range producer.Errors() {
			logger.Log.Error("[KAFKA INFRA] Kafka Async Write Error",
				zap.Error(err.Err),
				zap.Any("msg", err.Msg),
			)
		}
	}()
	return producer
}

// 一组协作消费同一个topic的消费者实例
func InitKafkaConsumerGroup(brokers []string, groupID string) sarama.ConsumerGroup {
	config := sarama.NewConfig()

	// OffsetOldest: If the group is new, start from the very beginning.
	// This ensures we don't miss messages sent while the consumer was down.
	// OffsetNewest: discard history
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	config.Consumer.Fetch.Default = 5 * 1024 * 1024
	config.Consumer.Return.Errors = true

	// Connect to Brokers
	group, err := sarama.NewConsumerGroup(brokers, groupID, config)
	if err != nil {
		logger.Log.Panic("[KAFKA INFRA] Failed to create consumer group", zap.Error(err))
	}

	go func() {
		for err := range group.Errors() {
			logger.Log.Error("[KAFKA INFRA] Consumer Group Error", zap.Error(err))
		}
	}()

	return group
}

func PushtoInput(producer sarama.AsyncProducer, topic string, payload []byte) {
	// 'payload' is already a JSON bytes containing user info & content.
	msg := &sarama.ProducerMessage{
		Topic: topic,
		//Kafka is a byte logging system that deal with binary bytes stream
		Value: sarama.ByteEncoder(payload),
	}
	producer.Input() <- msg

	// 不阻塞策略
	// select {
	// // Async send (Non-blocking)
	// case producer.Input() <- msg:
	// default:
	// 	// [CIRCUIT BREAKER]
	// 	// If Sarama's internal buffer (Channel) is full, we DROP the message.
	// 	// This prevents the Manager from hanging and ensures real-time broadcast (Redis)
	// 	// remains unaffected even if Kafka is slow or down.
	// 	logger.Log.Warn("[KAFKA INFRA] Kafka input buffer full, dropping persistence message")
	// }
}
