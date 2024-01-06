package example

import (
	"testing"

	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/rabbitmqx"
	"github.com/sivaosorg/rmqconn"
)

func createConn() (*rmqconn.RabbitMq, dbx.Dbx) {
	return rmqconn.NewClient(*rabbitmqx.GetRabbitMqConfigSample().SetDebugMode(false).SetEnabled(true).SetPort(5672))
}

func TestConn(t *testing.T) {
	_, s := createConn()
	logger.Infof("Rabbit connection status: %v", s)
}
