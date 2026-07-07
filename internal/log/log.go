package log

import (
	"context"
	"io"
	"log/slog"
	"os"
	"strings"
)

// levelVar backs the active log level so it can be adjusted at runtime.
var levelVar = new(slog.LevelVar)

// Options configure logger construction.
type Options struct {
	Level   string    // debug | info | warn | error
	Format  string    // text | json
	Service string    // e.g. "tracker"
	Version string    // git sha or "dev"
	Output  io.Writer // defaults to os.Stdout when nil
}

// New builds a logger. JSON handler when Format=="json", text otherwise. Every
// line carries service and version attributes.
func New(opts Options) *slog.Logger {
	levelVar.Set(parseLevel(opts.Level))

	w := opts.Output
	if w == nil {
		w = os.Stdout
	}
	ho := &slog.HandlerOptions{Level: levelVar}

	var h slog.Handler
	if strings.EqualFold(opts.Format, "json") {
		h = slog.NewJSONHandler(w, ho)
	} else {
		h = slog.NewTextHandler(w, ho)
	}

	return slog.New(h).With(
		slog.String("service", opts.Service),
		slog.String("version", opts.Version),
	)
}

// SetLevel changes the active log level for all loggers built by New.
func SetLevel(level string) { levelVar.Set(parseLevel(level)) }

func parseLevel(s string) slog.Level {
	switch strings.ToLower(s) {
	case "debug":
		return slog.LevelDebug
	case "warn":
		return slog.LevelWarn
	case "error":
		return slog.LevelError
	default:
		return slog.LevelInfo
	}
}

type ctxKey struct{}

// IntoContext returns a context carrying logger for retrieval via FromContext.
func IntoContext(ctx context.Context, logger *slog.Logger) context.Context {
	return context.WithValue(ctx, ctxKey{}, logger)
}

// FromContext returns the logger stored in ctx, or slog.Default if absent.
func FromContext(ctx context.Context) *slog.Logger {
	if l, ok := ctx.Value(ctxKey{}).(*slog.Logger); ok && l != nil {
		return l
	}
	return slog.Default()
}

// WithFields returns a context whose logger is augmented with the given attrs.
func WithFields(ctx context.Context, args ...any) context.Context {
	return IntoContext(ctx, FromContext(ctx).With(args...))
}
