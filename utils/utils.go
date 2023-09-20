package utils

import (
	log "github.com/sirupsen/logrus"
	"os"
)

func HandleError(err error, logger *log.Logger, level log.Level) {
	if err != nil {
		if level == log.DebugLevel {
			log.Debug(err.Error())
		} else if level == log.InfoLevel {
			log.Info(err.Error())
		} else if level == log.FatalLevel {
			log.Fatalln(err.Error())
		} else if level == log.ErrorLevel {
			log.Error(err.Error())
		} else if level == log.PanicLevel {
			log.Panic(err.Error())
		} else if level == log.TraceLevel {
			log.Traceln(err.Error())
		}
	}
}

func GetLogger(level log.Level, out *os.File) *log.Logger {
	return &log.Logger{
		Out:          out,
		ReportCaller: true,
		Level:        level,
		Formatter: &log.TextFormatter{
			//DisableColors:   true,
			//TimestampFormat: "2006-01-02 15:04:05",
			//FullTimestamp:   true,
			ForceColors: true,
		},
	}
}
