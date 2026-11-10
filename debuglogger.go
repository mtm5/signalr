package signalr

import "github.com/go-kit/log"

// noDebugLogger is the debug logger when debug logging is off. withDebugPrefix keeps it unwrapped,
// so debugEnabled can tell and callers can skip building log arguments.
type noDebugLogger struct{}

func (noDebugLogger) Log(...interface{}) error { return nil }

func debugEnabled(dbg StructuredLogger) bool {
	_, off := dbg.(noDebugLogger)
	return !off
}

func withDebugPrefix(dbg StructuredLogger, keyvals ...interface{}) StructuredLogger {
	if !debugEnabled(dbg) {
		return dbg
	}
	return log.WithPrefix(dbg, keyvals...)
}
