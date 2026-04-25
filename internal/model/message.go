package model

import (
	"encoding/json"
	"time"
)

const (
	TypeDanmaku = 101
	TypeStatus  = 102
	ActionLike  = 103
)

type WsPacket struct {
	Type   int             `json:"type"`
	RoomID string          `json:"room_id,omitempty"`
	Data   json.RawMessage `json:"data"`
}

type DanmakuMessage struct {
	ID       uint64    `gorm:"primaryKey;autoIncrement" json:"id"`
	RoomID   string    `gorm:"type:varchar(50);not null;index:idx_room_time" json:"room_id"`
	UserID   uint64    `gorm:"not null;index" json:"user_id"`
	Content  string    `gorm:"type:varchar(500);not null" json:"content"`
	SendTime time.Time `gorm:"type:datetime(3);not null;index:idx_room_time" json:"send_time"`
}

func (DanmakuMessage) TableName() string {
	return "danmaku_messages"
}

// for TypeStatus
type StatsData struct {
	Online uint64 `json:"online"`
	Likes  uint64 `json:"likes"`
}

// for ActionLike
type Like struct {
	Count uint64 `json:"count"`
}