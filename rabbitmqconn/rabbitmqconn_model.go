package rabbitmqconn

import (
	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/sivaosorg/govm/rabbitmqx"
)

type RabbitMq struct {
	Config  rabbitmqx.RabbitMqConfig `json:"config,omitempty"`
	conn    *amqp.Connection         `json:"-"`
	channel *amqp.Channel            `json:"-"`
	close   bool                     `json:"-"`
}
