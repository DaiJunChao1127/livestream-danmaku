package logger

import (
	"os"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
)

var Log *zap.Logger

type Env string

const (
	EnvDev  Env = "dev"
	EnvProd Env = "prod"
)

func InitLogger(env Env) {
	var config zap.Config

	if env == EnvProd {
		config = zap.NewProductionConfig()
		config.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	} else {
		config = zap.NewDevelopmentConfig()
		config.EncoderConfig.EncodeLevel = zapcore.CapitalColorLevelEncoder
		config.EncoderConfig.TimeKey = ""
		config.EncoderConfig.CallerKey = ""
	}

	var err error
	Log, err = config.Build()
	if err != nil {
		os.Exit(1)
	}

	zap.ReplaceGlobals(Log)
}

func Sync() {
	_ = Log.Sync()
}
