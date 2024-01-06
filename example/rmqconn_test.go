package example

import (
	"os"
	"testing"

	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/rmqconn"

	amqp "github.com/rabbitmq/amqp091-go"
)

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
	// using goroutines to consume all messages
	// go svc.ConsumeConf(callback)
	// or
	// go func(){ err = svc.ConsumeConf(callback) }
	err = svc.ConsumeConf(callback)
	if err != nil {
		logger.Errorf("Consuming message got an error", err)
		return
	}
	os.Exit(0)
}
