package logger

import (
	"compress/gzip"
	"io"
	"os"
	"path"
	"time"

	"github.com/lestrrat-go/file-rotatelogs"
	"github.com/op/go-logging"
)

const (
	LOG_ROTATION_INTERVAL = 24 * time.Hour       // every day
	LOG_MAX_AGE           = 365 * 24 * time.Hour // every year
	LOG_FORMAT            = "%{time:2006-01-02 15:04:05.000} [%{level:.4s}] %{shortfile} %{message}"
	LOG_COLOR_FORMAT      = "%{color}%{time:2006-01-02 15:04:05.000} [%{level:.4s}]%{color:reset} %{shortfile} %{message}"
)

var (
	stdoutBackend   logging.Backend
	fileBackend     logging.Backend
	syslogBackend   logging.Backend
	rsyslogBackends []logging.Backend
)

func EnableStdoutLog() {
	if stdoutBackend != nil {
		return
	}
	stdoutBackend = logging.NewBackendFormatter(
		logging.NewLogBackend(os.Stdout, "", 0),
		logging.MustStringFormatter(LOG_COLOR_FORMAT),
	)
	applyBackendChange()
}

func compressFile(filename string) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	gzfile, err := os.OpenFile(filename+".gz", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer gzfile.Close()

	gzWriter := gzip.NewWriter(gzfile)
	defer gzWriter.Close()

	if _, err := io.Copy(gzWriter, file); err != nil {
		return err // probably disk full
	}

	os.Remove(filename)
	return nil
}

func EnableFileLog(logPath string) error {
	dir := path.Dir(logPath)
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) {
			os.MkdirAll(dir, 0755)
		} else {
			return err
		}
	}

	rotationHandler := rotatelogs.HandlerFunc(func(e rotatelogs.Event) {
		if e.Type() != rotatelogs.FileRotatedEventType {
			return
		}
		filename := e.(*rotatelogs.FileRotatedEvent).PreviousFile()
		if err := compressFile(filename); err != nil {
			os.Remove(filename + ".gz")
		}
	})
	ioWriter, err := rotatelogs.New(
		logPath+".%Y-%m-%d",
		rotatelogs.WithLinkName(logPath),
		rotatelogs.WithMaxAge(LOG_MAX_AGE),
		rotatelogs.WithRotationTime(LOG_ROTATION_INTERVAL),
		rotatelogs.WithHandler(rotationHandler),
	)
	if err != nil {
		return err
	}

	fileBackend = logging.NewBackendFormatter(
		logging.NewLogBackend(ioWriter, "", 0),
		logging.MustStringFormatter(LOG_FORMAT),
	)
	applyBackendChange()
	return nil
}

func applyBackendChange() {
	var backends []logging.Backend
	if stdoutBackend != nil {
		backends = append(backends, stdoutBackend)
	}
	if fileBackend != nil {
		backends = append(backends, fileBackend)
	}
	if syslogBackend != nil {
		backends = append(backends, syslogBackend)
	}
	backends = append(backends, rsyslogBackends...)
	level := logging.GetLevel("")
	logging.SetBackend(backends...)
	logging.SetLevel(level, "")
}
