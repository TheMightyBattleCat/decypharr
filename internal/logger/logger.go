package logger

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/rs/zerolog"
	"github.com/sirrobot01/decypharr/internal/config"
	"gopkg.in/natefinch/lumberjack.v2"
)

var (
	once   sync.Once
	logger zerolog.Logger

	rotatingLogFileOnce sync.Once
	rotatingLogFile     *lumberjack.Logger
)

func GetLogPath() string {
	logsDir := filepath.Join(config.GetMainPath(), "logs")

	if _, err := os.Stat(logsDir); os.IsNotExist(err) {
		if err := os.MkdirAll(logsDir, 0755); err != nil {
			panic(fmt.Sprintf("Failed to create logs directory: %v", err))
		}
	}

	return logsDir
}

// sharedRotatingLogFile returns the process-wide lumberjack writer. All
// component loggers share one rotator so they don't race on the same file
// (each *lumberjack.Logger runs its own mill goroutine and rotation cycle).
func sharedRotatingLogFile() *lumberjack.Logger {
	rotatingLogFileOnce.Do(func() {
		rotatingLogFile = &lumberjack.Logger{
			Filename:   filepath.Join(GetLogPath(), "decypharr.log"),
			MaxSize:    10,
			MaxAge:     15,
			MaxBackups: 10,
			Compress:   true,
		}
	})
	return rotatingLogFile
}

// New returns a logger for one component, writing to two sinks:
//
//   - stdout (the journal under systemd): one short human line per event at
//     log_level (see humanWriter), coloured per log_color.
//   - logs/decypharr.log: every field of every event at log_file_level
//     (log_level when unset), in the greppable
//     "2006-01-02 15:04:05 | LEVEL | [prefix] message key=value" form.
//
// Running the console at info and the file at debug gives a readable live
// journal without losing any diagnostic detail on disk.
func New(prefix string) zerolog.Logger {
	cfg := config.Get()
	consoleLevel := parseLevel(cfg.LogLevel, zerolog.InfoLevel)
	fileLevel := parseLevel(cfg.LogFileLevel, consoleLevel)

	console := levelWriter{
		w:   newHumanWriter(os.Stdout, prefix, useColor(cfg.LogColor, os.Getenv, os.Stdout)),
		min: consoleLevel,
	}
	file := levelWriter{w: newFileWriter(sharedRotatingLogFile(), prefix), min: fileLevel}

	return zerolog.New(zerolog.MultiLevelWriter(console, file)).
		With().
		Timestamp().
		Logger().
		Level(min(consoleLevel, fileLevel))
}

// newFileWriter renders the log file's line format, unchanged so existing
// greps and analysis scripts keep working. Never coloured.
func newFileWriter(out io.Writer, prefix string) zerolog.ConsoleWriter {
	return zerolog.ConsoleWriter{
		Out:        out,
		TimeFormat: "2006-01-02 15:04:05",
		NoColor:    true,
		FormatLevel: func(i any) string {
			return strings.ToUpper(fmt.Sprintf("| %-6s|", i))
		},
		FormatMessage: func(i any) string {
			return fmt.Sprintf("[%s] %v", prefix, i)
		},
	}
}

func Default() zerolog.Logger {
	once.Do(func() {
		logger = New("decypharr")
	})
	return logger
}
