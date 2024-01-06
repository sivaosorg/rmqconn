package example

import (
	"testing"

	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/rabbitmqx"
	"github.com/sivaosorg/rmqconn"
)

func TestServiceDeclareExchange(t *testing.T) {
	r, _ := createConn()
	r.Config.Message.SetEnabled(true)
	message := rabbitmqx.GetRabbitMqMessageConfigSample()
	logger.Infof("message conf requesting: %v", message)
	svc := rmqconn.NewRmqClusterService(r)
	err := svc.DeclareExchange(*message)
	if err != nil {
		logger.Errorf("Declaring exchange conf got an error", err)
		return
	}
	logger.Infof("Declared exchange conf successfully")
}

func TestServiceProduceMessage(t *testing.T) {
	r, _ := createConn()
	r.Config.Message.SetEnabled(true)
	message := rabbitmqx.GetRabbitMqMessageConfigSample()
	logger.Infof("message conf requesting: %v", message)
	svc := rmqconn.NewRmqClusterService(r)
	err := svc.Produce(*message, "Hello 1 RabbitMQ!")
	if err != nil {
		logger.Errorf("Producing message got an error", err)
		return
	}
	err = svc.Produce(*message, "Hello 2 RabbitMQ!")
	if err != nil {
		logger.Errorf("Producing message got an error", err)
		return
	}
	svc.ZookeeperExchangeNoop()
	logger.Infof("Produced message successfully")
}

func TestServiceConsumeMessage(t *testing.T) {
	r, _ := createConn()
	r.Config.Message.SetEnabled(true)
	message := rabbitmqx.GetRabbitMqMessageConfigSample()
	logger.Infof("message conf requesting: %v", message)
	svc := rmqconn.NewRmqClusterService(r)
	err := svc.Consume(*message, nil)
	if err != nil {
		logger.Errorf("Consuming message got an error", err)
		return
	}
}
