package example

import (
	"os"
	"testing"

	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/rabbitmqx"
	"github.com/sivaosorg/rmqconn"

	amqp "github.com/rabbitmq/amqp091-go"
)

func createConn() (*rmqconn.RabbitMq, dbx.Dbx) {
	return rmqconn.NewClient(*rabbitmqx.GetRabbitMqConfigSample().SetDebugMode(false).SetEnabled(true).SetPort(5672))
}

func TestConn(t *testing.T) {
	_, s := createConn()
	logger.Infof("Rabbit connection status: %v", s)
}

func TestCoreServiceDeclareExchangeConf(t *testing.T) {
	r, _ := createConn()
	r.Config.Message.SetEnabled(true)
	svc := rmqconn.NewRmqCoreService(r)
	err := svc.DeclareExchangeConf()
	if err != nil {
		logger.Errorf("Declaring exchange conf got an error", err)
		return
	}
	logger.Infof("Declared exchange conf successfully")
}

func TestCoreServiceProduceMessage(t *testing.T) {
	r, _ := createConn()
	r.Config.Message.SetEnabled(true)
	svc := rmqconn.NewRmqCoreService(r)
	err := svc.DeclareExchangeConf()
	if err != nil {
		logger.Errorf("Declaring exchange conf got an error", err)
		return
	}
	err = svc.ProduceConf("Hello 1 RabbitMQ!")
	if err != nil {
		logger.Errorf("Producing message got an error", err)
		return
	}
	err = svc.ProduceConf("Hello 2 RabbitMQ!")
	if err != nil {
		logger.Errorf("Producing message got an error", err)
		return
	}
	logger.Infof("Produced message successfully")
}

func TestCoreServiceConsumeMessage(t *testing.T) {
	r, _ := createConn()
	r.Config.Message.SetEnabled(true)
	svc := rmqconn.NewRmqCoreService(r)
	err := svc.DeclareExchangeConf()
	if err != nil {
		logger.Errorf("Declaring exchange conf got an error", err)
		return
	}
	callback := func(next amqp.Delivery) {
		logger.Debugf("Received Exchange Name: %v, Message: %s", next.Exchange, string(next.Body))
	}
	err = svc.ConsumeConf(callback)
	if err != nil {
		logger.Errorf("Consuming message got an error", err)
		return
	}
	os.Exit(0)
}
