package log

import "github.com/sirupsen/logrus"

var logger = logrus.New()

func NewLogger(module string) logrus.FieldLogger {
	return logger.WithField("module", module)
}

func SetLevel(level string) {
	l, err := logrus.ParseLevel(level)
	if err != nil {
		panic(err)
	}
	logger.SetLevel(l)
}
