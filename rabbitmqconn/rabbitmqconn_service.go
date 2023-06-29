package rabbitmqconn

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sivaosorg/govm/utils"
)

type RabbitMqService interface {
	CreateTopic(topic string) error
	RemoveTopic(topic string) error
	Producer(topic string, message interface{}) error
	Consumer(topics []string, callback func(topic, message string)) error
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
		topic,
		"topic",
		true,
		false,
		false,
		false,
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
	err := r.rabbitmqConn.channel.Publish(
		topic,
		"",
		false,
		false,
		amqp.Publishing{
			ContentType: "text/plain", // application/json, text/plain
			Body:        []byte(utils.ToJson(message)),
		},
	)
	return err
}

func (r *rabbitmqServiceImpl) Consumer(topics []string, callback func(topic, message string)) error {
	queue, err := r.rabbitmqConn.channel.QueueDeclare(
		"",
		false,
		false,
		true,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	for _, topic := range topics {
		err = r.rabbitmqConn.channel.QueueBind(
			queue.Name,
			"",
			topic,
			false,
			nil,
		)
		if err != nil {
			return err
		}
	}
	messages, err := r.rabbitmqConn.channel.Consume(
		queue.Name,
		"",
		true,
		false,
		false,
		false,
		nil,
	)
	if err != nil {
		return err
	}
	go func(messages <-chan amqp.Delivery) {
		for msg := range messages {
			topic := msg.RoutingKey
			message := string(msg.Body)
			callback(topic, message)
		}
	}(messages)
	return nil
}
