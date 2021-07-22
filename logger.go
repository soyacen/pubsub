package easypubsub

import (
	"fmt"
	"io"
	"log"
)

var defaultLogger Logger = &nopLogger{}

func DefaultLogger() Logger {
	return defaultLogger
}

type Logger interface {
	Log(args ...interface{})
	Logf(format string, args ...interface{})
}

type nopLogger struct{}

func (l *nopLogger) Log(args ...interface{}) {}

func (l *nopLogger) Logf(format string, args ...interface{}) {}

type StdLogger struct {
	logger *log.Logger
}

func (s *StdLogger) Log(args ...interface{}) {
	s.logger.Output(2, fmt.Sprintln(args...))
}

func (s *StdLogger) Logf(format string, args ...interface{}) {
	s.logger.Output(2, fmt.Sprintf(format, args...))

}

func NewStdLogger(writer io.Writer) *StdLogger {
	return &StdLogger{logger: log.New(writer, "easypubsub ", log.LstdFlags|log.Lshortfile)}
}
