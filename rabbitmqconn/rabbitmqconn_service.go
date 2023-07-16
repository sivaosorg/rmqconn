package rabbitmqconn

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sivaosorg/govm/rabbitmqx"
	"github.com/sivaosorg/govm/utils"
)

type RabbitMqService interface {
	CreateTopic(topic string) error
	RemoveTopic(topic string) error
	Producer(topic string, message interface{}) error
	Consumer(topic, queue string, callback func(next amqp.Delivery)) error
}

type RabbitMqCoreService interface {
	RemoveExchange(exchangeName string) error
	DeclareExchangeConf() error
	DeclareExchangeWith(exchangeName string, exchangeType string, durable bool) error
	DeclareQueueConf() (amqp.Queue, error)
	DeclareQueueWith(queueName string, durable bool) (amqp.Queue, error)
	BindQueueExchangeConf() error
	BindQueueExchangeWith(queueName, exchangeName string) error
	ProduceConf(message interface{}) error
	ProduceWith(exchangeName string, exchangeType string, durable bool, message interface{}) error
	ConsumeConf(callback func(next amqp.Delivery)) error
	ConsumeWith(queueName string, exchangeName, exchangeType string, durable bool, callback func(next amqp.Delivery)) error
}

type rabbitmqCoreServiceImpl struct {
	rabbitmqConn *RabbitMq `json:"-"`
}

type rabbitmqServiceImpl struct {
	rabbitmqConn *RabbitMq `json:"-"`
}

func NewRabbitMqService(rabbitmqConn *RabbitMq) RabbitMqService {
	s := &rabbitmqServiceImpl{
		rabbitmqConn: rabbitmqConn,
	}
	return s
}

func NewRabbitMqCoreService(rabbitmqConn *RabbitMq) RabbitMqCoreService {
	s := &rabbitmqCoreServiceImpl{
		rabbitmqConn: rabbitmqConn,
	}
	return s
}

func (r *rabbitmqServiceImpl) CreateTopic(topic string) error {
	err := r.rabbitmqConn.channel.ExchangeDeclare(
		topic,               // name exchange
		amqp.ExchangeFanout, // type exchange
		true,                // Durable
		false,               // Auto-deleted
		false,               // Internal
		false,               // No-wait
		nil,
	)
	return err
}

func (r *rabbitmqServiceImpl) RemoveTopic(topic string) error {
	err := r.rabbitmqConn.channel.ExchangeDelete(
		topic,
		false,
		false,
	)
	return err
}

func (r *rabbitmqServiceImpl) Producer(topic string, message interface{}) error {
	err := r.rabbitmqConn.channel.ExchangeDeclare(
		topic,               // name exchange
		amqp.ExchangeFanout, // type exchange
		true,                // Durable
		false,               // Auto-deleted
		false,               // Internal
		false,               // No-wait
		nil,                 // Arguments
	)
	if err != nil {
		return err
	}
	err = r.rabbitmqConn.channel.Publish(
		topic,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json", // application/json, text/plain
			Body:        []byte(utils.ToJson(message)),
		},
	)
	return err
}

func (r *rabbitmqServiceImpl) Consumer(topic, queue string, callback func(next amqp.Delivery)) error {
	err := r.rabbitmqConn.channel.ExchangeDeclare(
		topic,               // name exchange
		amqp.ExchangeFanout, // type exchange
		true,                // Durable
		false,               // Auto-deleted
		false,               // Internal
		false,               // No-wait
		nil,                 // Arguments
	)
	if err != nil {
		return err
	}
	q, err := r.rabbitmqConn.channel.QueueDeclare(
		queue, // name queue
		true,  // Durable
		false, // Delete when unused
		false, // Exclusive
		false, // No-wait
		nil,   // Arguments
	)
	if err != nil {
		return err
	}
	err = r.rabbitmqConn.channel.QueueBind(
		q.Name, // name queue
		"",     // Routing key
		topic,  // name exchange
		false,
		nil,
	)
	if err != nil {
		return err
	}
	msg, err := r.rabbitmqConn.channel.Consume(
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
			callback(d)
		}
	}()
	_logger.Info("Consumer is waiting for messages (%s)...", topic)
	<-forever
	return nil
}

func (r *rabbitmqCoreServiceImpl) RemoveExchange(exchangeName string) error {
	err := r.rabbitmqConn.channel.ExchangeDelete(
		exchangeName,
		false,
		false,
	)
	return err
}

func (r *rabbitmqCoreServiceImpl) DeclareExchangeConf() error {
	rabbitmqx.RabbitMqExchangeConfigValidator(&r.rabbitmqConn.Config.Message.Exchange)
	return r.DeclareExchangeWith(r.rabbitmqConn.Config.Message.Exchange.Name,
		r.rabbitmqConn.Config.Message.Exchange.Kind,
		r.rabbitmqConn.Config.Message.Exchange.Durable)
}

func (r *rabbitmqCoreServiceImpl) DeclareExchangeWith(exchangeName string, exchangeType string, durable bool) error {
	config := rabbitmqx.NewRabbitMqExchangeConfig().SetName(exchangeName).SetKind(exchangeType).SetDurable(durable)
	err := r.rabbitmqConn.channel.ExchangeDeclare(
		config.Name,    // name exchange
		config.Kind,    // type exchange
		config.Durable, // Durable
		false,          // Auto-deleted
		false,          // Internal
		false,          // No-wait
		nil,
	)
	return err
}

func (r *rabbitmqCoreServiceImpl) DeclareQueueConf() (amqp.Queue, error) {
	rabbitmqx.RabbitMqQueueConfigValidator(&r.rabbitmqConn.Config.Message.Queue)
	return r.DeclareQueueWith(r.rabbitmqConn.Config.Message.Queue.Name, r.rabbitmqConn.Config.Message.Queue.Durable)
}

func (r *rabbitmqCoreServiceImpl) DeclareQueueWith(queueName string, durable bool) (amqp.Queue, error) {
	config := rabbitmqx.NewRabbitMqQueueConfig().SetName(queueName).SetDurable(durable)
	q, err := r.rabbitmqConn.channel.QueueDeclare(
		config.Name,    // name queue
		config.Durable, // Durable
		false,          // Delete when unused
		false,          // Exclusive
		false,          // No-wait
		nil,            // Arguments
	)
	return q, err
}

func (r *rabbitmqCoreServiceImpl) BindQueueExchangeConf() error {
	rabbitmqx.RabbitMqExchangeConfigValidator(&r.rabbitmqConn.Config.Message.Exchange)
	rabbitmqx.RabbitMqQueueConfigValidator(&r.rabbitmqConn.Config.Message.Queue)
	return r.BindQueueExchangeWith(r.rabbitmqConn.Config.Message.Queue.Name, r.rabbitmqConn.Config.Message.Exchange.Name)
}

func (r *rabbitmqCoreServiceImpl) BindQueueExchangeWith(queueName, exchangeName string) error {
	exchange := rabbitmqx.NewRabbitMqExchangeConfig().SetName(exchangeName)
	queue := rabbitmqx.NewRabbitMqQueueConfig().SetName(queueName)
	err := r.rabbitmqConn.channel.QueueBind(
		queue.Name,    // name queue
		"",            // Routing key
		exchange.Name, // name exchange
		false,
		nil,
	)
	return err
}

func (r *rabbitmqCoreServiceImpl) ProduceConf(message interface{}) error {
	if !r.rabbitmqConn.Config.Message.IsEnabled {
		return fmt.Errorf("ProduceConf, message unavailable (enabled = false)")
	}
	rabbitmqx.RabbitMqExchangeConfigValidator(&r.rabbitmqConn.Config.Message.Exchange)
	return r.ProduceWith(r.rabbitmqConn.Config.Message.Exchange.Name,
		r.rabbitmqConn.Config.Message.Exchange.Kind,
		r.rabbitmqConn.Config.Message.Exchange.Durable,
		message)
}

func (r *rabbitmqCoreServiceImpl) ProduceWith(exchangeName string, exchangeType string, durable bool, message interface{}) error {
	err := r.DeclareExchangeWith(exchangeName, exchangeType, durable)
	if err != nil {
		return err
	}
	err = r.rabbitmqConn.channel.Publish(
		exchangeName,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "application/json", // application/json, text/plain
			Body:        []byte(utils.ToJson(message)),
		},
	)
	return err
}

func (r *rabbitmqCoreServiceImpl) ConsumeConf(callback func(next amqp.Delivery)) error {
	if !r.rabbitmqConn.Config.Message.IsEnabled {
		return fmt.Errorf("ConsumeConf, message unavailable (enabled = false)")
	}
	rabbitmqx.RabbitMqExchangeConfigValidator(&r.rabbitmqConn.Config.Message.Exchange)
	rabbitmqx.RabbitMqQueueConfigValidator(&r.rabbitmqConn.Config.Message.Queue)
	return r.ConsumeWith(r.rabbitmqConn.Config.Message.Queue.Name,
		r.rabbitmqConn.Config.Message.Exchange.Name,
		r.rabbitmqConn.Config.Message.Exchange.Kind,
		r.rabbitmqConn.Config.Message.Exchange.Durable,
		callback)
}

func (r *rabbitmqCoreServiceImpl) ConsumeWith(queueName string, exchangeName, exchangeType string, durable bool, callback func(next amqp.Delivery)) error {
	err := r.DeclareExchangeWith(exchangeName, exchangeType, durable)
	if err != nil {
		return err
	}
	q, err := r.DeclareQueueWith(queueName, durable)
	if err != nil {
		return err
	}
	err = r.BindQueueExchangeWith(q.Name, exchangeName)
	if err != nil {
		return err
	}
	msg, err := r.rabbitmqConn.channel.Consume(
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
			callback(d)
		}
	}()
	_logger.Info("Consumer is waiting for messages (%s)...", exchangeName)
	<-forever
	return nil
}
