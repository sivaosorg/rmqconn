package rabbitmqconn

import (
	amqp "github.com/rabbitmq/amqp091-go"
)

type RabbitMq struct {
	conn    *amqp.Connection `json:"-"`
	channel *amqp.Channel    `json:"-"`
	close   bool             `json:"-"`
}
