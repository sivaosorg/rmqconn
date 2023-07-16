package rabbitmqconn

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sivaosorg/govm/rabbitmqx"
)

type RabbitMq struct {
	config  rabbitmqx.RabbitMqConfig `json:"-"`
	conn    *amqp.Connection         `json:"-"`
	channel *amqp.Channel            `json:"-"`
	close   bool                     `json:"-"`
}
