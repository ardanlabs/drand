package testlogger

import (
	"os"
	"testing"

	"github.com/drand/drand/log"
)

// Level returns the level to default the logger based on the DRAND_TEST_LOGS presence
func Level(t *testing.T) int {
	logLevel := log.LogInfo
	debugEnv, isDebug := os.LookupEnv("DRAND_TEST_LOGS")
	if isDebug && debugEnv == "DEBUG" {
		t.Log("Enabling LogDebug logs")
		logLevel = log.LogDebug
	}

	return logLevel
}

// New returns a configured logger
func New(t *testing.T) log.Logger {
	return log.NewJSONLogger(nil, Level(t)).
		With("testName", t.Name())
}
