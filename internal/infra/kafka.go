// 高吞吐消息队列
package infra

import (
	"time"

	"github.com/DaiJunChao1127/livestream-danmaku/internal/logger"
	"github.com/IBM/sarama"
	"go.uber.org/zap"
)

const (
	ProducerChanBufSize = 16384 // 2^14
)

func InitKafkaProducer(brokers []string) sarama.AsyncProducer {
	config := sarama.NewConfig()

	config.ChannelBufferSize = ProducerChanBufSize
	//   - NoResponse 生产者发完不等确认
	//   - WaitForLocal: Leader returns OK after receiving 只要leader分区写入成功就回ACK
	//   - WaitForAll: all follower synced 要等ISR副本(合格副本)也同步完成才回ACK
	config.Producer.RequiredAcks = sarama.WaitForLocal

	config.Producer.Compression = sarama.CompressionSnappy

	// 攒批发送策略
	config.Producer.Flush.Messages = 50                     // 每批达到50条就可以触发发送
	config.Producer.Flush.MaxMessages = 200                 // 单批最多200条
	config.Producer.Flush.Frequency = 50 * time.Millisecond // 即使没攒够条数 最多等50ms也要发一批
	config.Producer.Flush.Bytes = 8 * 1024                  // 批次累计到8KB也会触发发送

	config.Producer.Return.Successes = false
	config.Producer.Return.Errors = true

	// 每个topic有多个partition
	// 一个topic被拆除好几段 每个分区就是一个patrtition
	config.Producer.Partitioner = sarama.NewRandomPartitioner

	// 连接到kafka服务端
	// 提供Kafka集群入口地址和config
	producer, err := sarama.NewAsyncProducer(brokers, config)

	if err != nil {
		logger.Log.Panic("[KAFKA INFRA]Failed to start Kafka async producer", zap.Error(err))
	}

	// goroutine
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

func InitKafkaConsumerGrop(brokers []string, groupID string) sarama.ConsumerGroup {
	config := sarama.NewConfig()

	// 不会丢失消息
	config.Consumer.Offsets.Initial = sarama.OffsetOldest

	// 单词默认拉取大小
	config.Consumer.Fetch.Default = 5 * 1024 * 1024 // 5MB

	config.Consumer.Return.Errors = true

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

// helper函数
func PushToInput(producer sarama.AsyncProducer, topic string, payload []byte) {
	// 组装发送到Kafka的消息对象
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.ByteEncoder(payload),
	}

	// producer.Input()返回一个只写通道
	// msg写入这个通道
	producer.Input() <- msg

	// // select: 监听多个通道 谁能执行就先执行谁
	// // 都阻塞就会走default 、
	// // 没有default select就会阻塞
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
