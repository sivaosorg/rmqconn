# RabbitMQConn

## Example RabbitMQ Pub.Sub

```go
package main

import (
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/rabbitmqx"
	"github.com/sivaosorg/rabbitmqconn/rabbitmqconn"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	test001()
}

func test001() {
	c, s := rabbitmqconn.NewClient(*rabbitmqx.GetRabbitMqConfigSample().SetDebugMode(false).SetEnabled(true).SetPort(5672))
	// c, s := rabbitmqconn.NewClient(*rabbitmqx.GetRabbitMqConfigSample().SetDebugMode(false).SetEnabled(true).SetPort(5672).SetUsername("guest1").SetPassword("guest1").SetUrlConn(""))
	logger.Infof("rabbitmq state:: %s", s.Json())
	defer c.Close()
	service := rabbitmqconn.NewRabbitMqService(c)
	topic := "topic_sample"
	err := service.CreateTopic(topic)
	if err != nil {
		panic(err)
	}
	err = service.Producer(topic, "Hello 1 RabbitMQ!")
	if err != nil {
		panic(err)
	}
	err = service.Producer(topic, "Hello 2 RabbitMQ!")
	if err != nil {
		panic(err)
	}
	callback := func(next amqp.Delivery) {
		logger.Infof("Received message - Topic: %s, Message: %s\n", next.Exchange, string(next.Body))
	}
	err = service.Consumer(topic, "queue_sample", callback)

}

func test002() {
	c, s := rabbitmqconn.NewClient(*rabbitmqx.GetRabbitMqConfigSample().SetDebugMode(false).SetEnabled(true).SetPort(5672))
	c.Config.Message.SetEnabled(true)
	logger.Infof("rabbitmq state:: %s", s.Json())
	logger.Infof("rabbitmq connection:: %s", c.Json())
	defer c.Close()
	service := rabbitmqconn.NewRabbitMqCoreService(c)
	// err := service.RemoveExchange(c.Config.Message.Exchange.Name)
	err := service.DeclareExchangeConf()
	if err != nil {
		panic(err)
	}

	err = service.ProduceConf("Hello 1 RabbitMQ!")
	err = service.ProduceConf(s)
	if err != nil {
		panic(err)
	}
	err = service.ProduceConf("Hello 2 RabbitMQ!")
	if err != nil {
		panic(err)
	}
	err = service.ProduceConf("Hello 3 RabbitMQ!")
	if err != nil {
		panic(err)
	}
	callback := func(next amqp.Delivery) {
		logger.Debugf("Received Exchange Name: %v, Message: %s\n", next.Exchange, string(next.Body))
	}
	err = service.ConsumeConf(callback)
}
```
