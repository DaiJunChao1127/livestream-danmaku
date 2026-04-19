package infra

import (
	"github.com/DaiJunChao1127/livestream-danmaku/internal/logger"
	"github.com/DaiJunChao1127/livestream-danmaku/internal/model"
	"go.uber.org/zap"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

func InitDB() *gorm.DB {
	// Data Source Name
	var dsn string = "root:root@tcp(127.0.0.1:3306)/danmaku_db?charset=utf8mb4&parseTime=True&loc=Local"
	db, err := gorm.Open(mysql.Open(dsn), &gorm.Config{})

	if err != nil {
		logger.Log.Fatal("[DB infra]Failed to connetct to database", zap.Error(err))
	}

	// DROP TABLE danmaku_messages;
	// model.DanmakuMessage 实现了 TableName()
	if err := db.Migrator().DropTable(&model.DanmakuMessage{}); err != nil {
		logger.Log.Error("[DB INFRA]Drop table DanmakuMessage failed", zap.Error(err))
	}
	if err := db.Migrator().DropTable(&model.Room{}); err != nil {
		logger.Log.Error("[DB INFRA]Drop table Room failed", zap.Error(err))
	}
	if err := db.Migrator().DropTable(&model.User{}); err != nil {
		logger.Log.Error("[DB INFRA]Drop table User failed", zap.Error(err))
	}

	// Auto-Migrate (Create tables if not exist)
	if err := db.AutoMigrate(&model.DanmakuMessage{}); err != nil {
		logger.Log.Fatal("[DB INFRA]Database table DanmakuMessage migration failed", zap.Error(err))
	}
	if err := db.AutoMigrate(&model.Room{}); err != nil {
		logger.Log.Fatal("[DB INFRA]Database table Room migration failed", zap.Error(err))
	}
	if err := db.AutoMigrate(&model.User{}); err != nil {
		logger.Log.Fatal("[DB INFRA]Database table User migration failed", zap.Error(err))
	}
	return db
}
