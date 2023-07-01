/*
说明：zap日志打印的封装
创建人 jettchen
创建时间 2023/07/01
*/
package log

import (
	"fmt"
	"os"
	"path/filepath"

	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

var (
	Logger *zap.Logger
)

func getEncoder() zapcore.Encoder {
	encoderConfig := zap.NewProductionEncoderConfig()
	encoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder
	encoderConfig.EncodeLevel = zapcore.CapitalLevelEncoder
	return zapcore.NewConsoleEncoder(encoderConfig)
}

func getLogWriter(fileName string, fileSize int, fileCount int, expireDay int) zapcore.WriteSyncer {
	dir := filepath.Dir(fileName)
	os.MkdirAll(dir, 0775)
	lumberJackLogger := &lumberjack.Logger{
		Filename:   fileName,
		MaxSize:    fileSize, // 单位是MB
		MaxBackups: fileCount,
		MaxAge:     expireDay,
		Compress:   false,
	}
	return zapcore.AddSync(lumberJackLogger)
}

func InitLog(fileName string, fileSize int, fileCount int, expireDay int, logLevel int) error {
	writeSyncer := getLogWriter(fileName, fileSize, fileCount, expireDay)
	encoder := getEncoder()
	core := zapcore.NewCore(encoder, writeSyncer, zapcore.Level(logLevel))

	Logger = zap.New(core, zap.AddCaller())
	return nil
}

func Debug(format string, args ...interface{}) {
	Logger.Debug(fmt.Sprintf(format, args...))
}

func Info(format string, args ...interface{}) {
	Logger.Info(fmt.Sprintf(format, args...))
}

func Warn(format string, args ...interface{}) {
	Logger.Warn(fmt.Sprintf(format, args...))
}

func Error(format string, args ...interface{}) {
	Logger.Error(fmt.Sprintf(format, args...))
}

func Errorf(err error, format string, args ...interface{}) {
	Logger.Error(fmt.Sprintf(format, args...), zap.Error(err))
}
