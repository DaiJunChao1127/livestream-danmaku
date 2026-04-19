package repo

import (
	"time"

	"github.com/DaiJunChao1127/livestream-danmaku/internal/model"
	"gorm.io/gorm"
)

type MessageRepo struct {
	db *gorm.DB
}

func NewMessageRepo(db *gorm.DB) *MessageRepo {
	return &MessageRepo{db: db}
}

func (r *MessageRepo) CreateInBatches(msgs []*model.DanmakuMessage) error {
	if len(msgs) == 0 {
		return nil
	}

	return r.db.CreateInBatches(msgs, len(msgs)).Error
}

func (r *MessageRepo) GetPlayBackDanmaku(roomID string, queryTime time.Time, limit int) ([]*model.DanmakuMessage, error) {
	var msgs []*model.DanmakuMessage

	// SELECT * FROM danmaku_messages WHERE rood_id = `1001`
	query := r.db.Where("room_id = ?", roomID)

	// "0001-01-01 00:00:00" empty time
	if !queryTime.IsZero() {
		query = query.Where("send_time >= ?", queryTime)
	}

	err := query.Order("send time ASC").
		Limit(limit).
		Find(&msgs).Error

	return msgs, err
}
