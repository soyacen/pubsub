package easypubsub

var defaultLogger Logger = &nopLogger{}

func DefaultLogger() Logger {
	return defaultLogger
}

type Logger interface {
	Debug(args ...interface{})
	Debugf(template string, args ...interface{})
	Info(args ...interface{})
	Infof(template string, args ...interface{})
	Error(args ...interface{})
	Errorf(template string, args ...interface{})
}

type nopLogger struct{}

func (l *nopLogger) Debug(args ...interface{}) {}

func (l *nopLogger) Debugf(template string, args ...interface{}) {}

func (l *nopLogger) Info(args ...interface{}) {}

func (l *nopLogger) Infof(template string, args ...interface{}) {}

func (l *nopLogger) Error(args ...interface{}) {}

func (l *nopLogger) Errorf(template string, args ...interface{}) {}
