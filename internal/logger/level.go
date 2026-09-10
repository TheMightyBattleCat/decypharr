package logger

import (
	"io"
	"strings"

	"github.com/rs/zerolog"
)

// levelWriter passes on only the events at or above min, so the console and
// the log file can record different levels behind one zerolog.Logger.
type levelWriter struct {
	w   io.Writer
	min zerolog.Level
}

func (l levelWriter) Write(p []byte) (int, error) { return l.w.Write(p) }

func (l levelWriter) WriteLevel(level zerolog.Level, p []byte) (int, error) {
	if level < l.min {
		return len(p), nil
	}
	return l.w.Write(p)
}

// parseLevel maps a configured level name to a zerolog level, or def when the
// name is empty or unknown.
func parseLevel(name string, def zerolog.Level) zerolog.Level {
	switch strings.ToLower(strings.TrimSpace(name)) {
	case "trace":
		return zerolog.TraceLevel
	case "debug":
		return zerolog.DebugLevel
	case "info":
		return zerolog.InfoLevel
	case "warn", "warning":
		return zerolog.WarnLevel
	case "error":
		return zerolog.ErrorLevel
	}
	return def
}
