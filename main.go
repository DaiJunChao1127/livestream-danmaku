package main

import (
	"github.com/DaiJunChao1127/livestream-danmaku/internal/logger"
)

func main() {
	logger.InitLogger(logger.EnvProd)
	logger.Log.Info("hello")
}