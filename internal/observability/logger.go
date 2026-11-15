package observability

import (
	"fmt"
	"log/slog"
	"os"
	"strings"
)

// NewLogger builds a structured logger. Development environments use text
// output for readability; every other environment emits JSON.
func NewLogger(environment, level string) (*slog.Logger, error) {
	var slogLevel slog.Level
	switch strings.ToLower(strings.TrimSpace(level)) {
	case "debug":
		slogLevel = slog.LevelDebug
	case "info", "":
		slogLevel = slog.LevelInfo
	case "warn":
		slogLevel = slog.LevelWarn
	case "error":
		slogLevel = slog.LevelError
	default:
		return nil, fmt.Errorf("unsupported log level %q", level)
	}

	opts := &slog.HandlerOptions{Level: slogLevel}
	var handler slog.Handler
	switch strings.ToLower(strings.TrimSpace(environment)) {
	case "development", "local", "test":
		handler = slog.NewTextHandler(os.Stdout, opts)
	default:
		handler = slog.NewJSONHandler(os.Stdout, opts)
	}

	return slog.New(handler).With("service", "durable-job-processor"), nil
}
