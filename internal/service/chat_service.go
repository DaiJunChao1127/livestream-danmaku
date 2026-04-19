package service

import (
	"time"

	"github.com/DaiJunChao1127/livestream-danmaku/internal/model"
	"github.com/DaiJunChao1127/livestream-danmaku/internal/repo"
)

type ChatService struct {
	repo *repo.MessageRepo
}

func NewChatService(repo *repo.MessageRepo) *ChatService {
	return &ChatService{repo: repo}
}

func (s *ChatService) GetPlayBackDanmaku(roomID string, startTime time.Time) ([]*model.DanmakuMessage, error) {
	const defaultLimit = 100

	return s.repo.GetPlayBackDanmaku(roomID, startTime, defaultLimit)
}
