package rabbitmqconn

import (
	"context"
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/rabbitmqx"
	"github.com/sivaosorg/govm/utils"
)

var callbackDefault = func(next amqp.Delivery) {
	logger.Debugf(fmt.Sprintf("Received exchange: %v, message (content-type: %s): %s", next.Exchange, next.ContentType, string(next.Body)))
}

type RabbitMqClusterService interface {
	RemoveExchange(message rabbitmqx.RabbitMqMessageConfig) error
	DeclareExchange(message rabbitmqx.RabbitMqMessageConfig) error
	DeclareQueue(message rabbitmqx.RabbitMqMessageConfig) (amqp.Queue, error)
	BindQueueExchange(message rabbitmqx.RabbitMqMessageConfig) error
	Produce(message rabbitmqx.RabbitMqMessageConfig, data interface{}) error
	Consume(message rabbitmqx.RabbitMqMessageConfig, callback func(next amqp.Delivery)) error
	GetByMap(clusters map[string]rabbitmqx.RabbitMqMessageConfig, key string) (rabbitmqx.RabbitMqMessageConfig, error)
	GetBySlice(clusters []rabbitmqx.MultiTenantRabbitMqConfig, key string) (rabbitmqx.MultiTenantRabbitMqConfig, error)
	ProduceByMap(clusters map[string]rabbitmqx.RabbitMqMessageConfig, key string, data interface{}) error
	ConsumeByMap(clusters map[string]rabbitmqx.RabbitMqMessageConfig, key string, callback func(next amqp.Delivery)) error
	ProduceBySlice(clusters []rabbitmqx.MultiTenantRabbitMqConfig, key string, usableMessageDefault bool, data interface{}) error
	ConsumeBySlice(clusters []rabbitmqx.MultiTenantRabbitMqConfig, key string, usableMessageDefault bool, callback func(next amqp.Delivery)) error
}

type rabbitMqClusterServiceImpl struct {
	c *RabbitMq
}

func NewRabbitMqClusterService(c *RabbitMq) RabbitMqClusterService {
	return &rabbitMqClusterServiceImpl{
		c: c,
	}
}

func (s *rabbitMqClusterServiceImpl) RemoveExchange(message rabbitmqx.RabbitMqMessageConfig) error {
	if !message.IsEnabled {
		return fmt.Errorf("Message (exchange: %s, queue: %s) unavailable", message.Exchange.Name, message.Queue.Name)
	}
	err := s.c.channel.ExchangeDelete(
		message.Exchange.Name,
		false,
		false,
	)
	return err
}

func (s *rabbitMqClusterServiceImpl) DeclareExchange(message rabbitmqx.RabbitMqMessageConfig) error {
	if !message.IsEnabled {
		return fmt.Errorf("Message (exchange: %s, queue: %s) unavailable", message.Exchange.Name, message.Queue.Name)
	}
	err := s.c.channel.ExchangeDeclare(
		message.Exchange.Name,    // name exchange
		message.Exchange.Kind,    // type exchange
		message.Exchange.Durable, // Durable
		false,                    // Auto-deleted
		false,                    // Internal
		false,                    // No-wait
		nil,
	)
	return err
}

func (s *rabbitMqClusterServiceImpl) DeclareQueue(message rabbitmqx.RabbitMqMessageConfig) (amqp.Queue, error) {
	if !message.IsEnabled {
		return amqp.Queue{}, fmt.Errorf("Message (exchange: %s, queue: %s) unavailable", message.Exchange.Name, message.Queue.Name)
	}
	q, err := s.c.channel.QueueDeclare(
		message.Queue.Name,    // name queue
		message.Queue.Durable, // Durable
		false,                 // Delete when unused
		false,                 // Exclusive
		false,                 // No-wait
		nil,                   // Arguments
	)
	return q, err
}

func (s *rabbitMqClusterServiceImpl) BindQueueExchange(message rabbitmqx.RabbitMqMessageConfig) error {
	if !message.IsEnabled {
		return fmt.Errorf("Message (exchange: %s, queue: %s) unavailable", message.Exchange.Name, message.Queue.Name)
	}
	err := s.c.channel.QueueBind(
		message.Queue.Name,    // name queue
		"",                    // Routing key
		message.Exchange.Name, // name exchange
		false,
		nil,
	)
	return err
}

func (s *rabbitMqClusterServiceImpl) Produce(message rabbitmqx.RabbitMqMessageConfig, data interface{}) error {
	if !message.IsEnabled {
		return fmt.Errorf("Message (exchange: %s, queue: %s) unavailable", message.Exchange.Name, message.Queue.Name)
	}
	err := s.DeclareExchange(message)
	if err != nil {
		return err
	}
	if s.c.Config.DebugMode {
		_logger.Info(fmt.Sprintf("Producer is running for messages (exchange: %s, queue: %s) outgoing data: %v", message.Exchange.Name, message.Queue.Name, utils.ToJson(data)))
	} else {
		_logger.Info(fmt.Sprintf("Producer is running for messages (exchange: %s, queue: %s)", message.Exchange.Name, message.Queue.Name))
	}
	err = s.c.channel.PublishWithContext(
		context.Background(),
		message.Exchange.Name,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json", // application/json, text/plain
			Body:        []byte(utils.ToJson(data)),
		},
	)
	return err
}

func (s *rabbitMqClusterServiceImpl) Consume(message rabbitmqx.RabbitMqMessageConfig, callback func(next amqp.Delivery)) error {
	if !message.IsEnabled {
		return fmt.Errorf("Message (exchange: %s, queue: %s) unavailable", message.Exchange.Name, message.Queue.Name)
	}
	err := s.DeclareExchange(message)
	if err != nil {
		return err
	}
	q, err := s.DeclareQueue(message)
	if err != nil {
		return err
	}
	err = s.BindQueueExchange(message)
	if err != nil {
		return err
	}
	msg, err := s.c.channel.Consume(
		q.Name, // name queue
		"",     // Consumer
		true,   // Auto-acknowledge
		false,  // Exclusive
		false,  // No-local
		false,  // No-wait
		nil,    // Arguments
	)
	if err != nil {
		return err
	}
	forever := make(chan bool)
	go func() {
		for d := range msg {
			if callback == nil {
				callbackDefault(d)
			} else {
				callback(d)
			}
		}
	}()
	_logger.Info(fmt.Sprintf("Consumer is waiting for messages (exchange: %s, queue: %s)...", message.Exchange.Name, q.Name))
	<-forever
	return nil
}

func (s *rabbitMqClusterServiceImpl) GetByMap(clusters map[string]rabbitmqx.RabbitMqMessageConfig, key string) (rabbitmqx.RabbitMqMessageConfig, error) {
	if len(clusters) == 0 {
		return rabbitmqx.RabbitMqMessageConfig{}, fmt.Errorf("Cluster is required")
	}
	if utils.IsEmpty(key) {
		return rabbitmqx.RabbitMqMessageConfig{}, fmt.Errorf("Key is required")
	}
	v, ok := clusters[key]
	if !ok {
		return rabbitmqx.RabbitMqMessageConfig{}, fmt.Errorf("Rabbitmq conf not found for key: %s", key)
	}
	return v, nil
}

func (s *rabbitMqClusterServiceImpl) GetBySlice(clusters []rabbitmqx.MultiTenantRabbitMqConfig, key string) (rabbitmqx.MultiTenantRabbitMqConfig, error) {
	if len(clusters) == 0 {
		return rabbitmqx.MultiTenantRabbitMqConfig{}, fmt.Errorf("Cluster is required")
	}
	if utils.IsEmpty(key) {
		return rabbitmqx.MultiTenantRabbitMqConfig{}, fmt.Errorf("Key is required")
	}
	for _, v := range clusters {
		if v.Key == key {
			return v, nil
		}
	}
	return rabbitmqx.MultiTenantRabbitMqConfig{}, fmt.Errorf("Rabbitmq conf not found for key: %s", key)
}

func (s *rabbitMqClusterServiceImpl) ProduceByMap(clusters map[string]rabbitmqx.RabbitMqMessageConfig, key string, data interface{}) error {
	v, err := s.GetByMap(clusters, key)
	if err != nil {
		return err
	}
	return s.Produce(v, data)
}

func (s *rabbitMqClusterServiceImpl) ConsumeByMap(clusters map[string]rabbitmqx.RabbitMqMessageConfig, key string, callback func(next amqp.Delivery)) error {
	v, err := s.GetByMap(clusters, key)
	if err != nil {
		return err
	}
	return s.Consume(v, callback)
}

func (s *rabbitMqClusterServiceImpl) ProduceBySlice(clusters []rabbitmqx.MultiTenantRabbitMqConfig, key string, usableMessageDefault bool, data interface{}) error {
	v, err := s.GetBySlice(clusters, key)
	if err != nil {
		return err
	}
	if usableMessageDefault {
		return s.Produce(v.Config.Message, data)
	}
	return s.ProduceByMap(v.Config.Clusters, key, data)
}

func (s *rabbitMqClusterServiceImpl) ConsumeBySlice(clusters []rabbitmqx.MultiTenantRabbitMqConfig, key string, usableMessageDefault bool, callback func(next amqp.Delivery)) error {
	v, err := s.GetBySlice(clusters, key)
	if err != nil {
		return err
	}
	if usableMessageDefault {
		return s.Consume(v.Config.Message, callback)
	}
	return s.ConsumeByMap(v.Config.Clusters, key, callback)
}
