package rabbitmqconn

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sivaosorg/govm/utils"
)

type RabbitMqService interface {
	CreateTopic(topic string) error
	RemoveTopic(topic string) error
	Producer(topic string, message interface{}) error
	Consumer(topic, queue string, callback func(next amqp.Delivery)) error
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
