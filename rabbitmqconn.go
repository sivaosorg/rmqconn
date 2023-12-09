package rabbitmqconn

import (
	"fmt"
	"os"

	"github.com/sivaosorg/govm/dbx"
	"github.com/sivaosorg/govm/logger"
	"github.com/sivaosorg/govm/rabbitmqx"
	"github.com/sivaosorg/govm/utils"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	_logger = logger.NewLogger()
)

func NewRabbitMq() *RabbitMq {
	r := &RabbitMq{}
	return r
}

func (r *RabbitMq) SetConn(value *amqp.Connection) *RabbitMq {
	r.conn = value
	return r
}

func (r *RabbitMq) SetChannel(value *amqp.Channel) *RabbitMq {
	r.channel = value
	return r
}

func (r *RabbitMq) SetConfig(value rabbitmqx.RabbitMqConfig) *RabbitMq {
	r.Config = value
	return r
}

func (r *RabbitMq) SetClose(value bool) *RabbitMq {
	r.close = value
	return r
}

func (r *RabbitMq) SetState(value dbx.Dbx) *RabbitMq {
	r.State = value
	return r
}

func (r *RabbitMq) GetConn() *amqp.Connection {
	return r.conn
}

func (r *RabbitMq) GetChannel() *amqp.Channel {
	return r.channel
}

func (r *RabbitMq) Json() string {
	return utils.ToJson(r)
}

func NewClient(config rabbitmqx.RabbitMqConfig) (*RabbitMq, dbx.Dbx) {
	instance := NewRabbitMq()
	s := dbx.NewDbx().SetDebugMode(config.DebugMode)
	if !config.IsEnabled {
		s.SetConnected(false).
			SetMessage("RabbitMQ unavailable").
			SetError(fmt.Errorf(s.Message))
		instance.SetState(*s)
		return instance, *s
	}
	if config.Timeout == 0 {
		config.SetTimeout(10)
	}
	// conn, err := amqp.Dial(config.ToUrlConn())
	conn, err := amqp.DialConfig(config.ToUrlConn(), amqp.Config{Dial: amqp.DefaultDial(config.Timeout)})
	if err != nil {
		s.SetConnected(false).SetError(err).SetMessage(err.Error())
		instance.SetState(*s)
		return instance, *s
	}
	channel, err := conn.Channel()
	if err != nil {
		s.SetConnected(false).SetError(err).SetMessage(err.Error())
		instance.SetState(*s)
		return instance, *s
	}
	if config.DebugMode {
		_logger.Info(fmt.Sprintf("RabbitMQ client connection:: %s", config.Json()))
		_logger.Info(fmt.Sprintf("Connected successfully to rabbitmq:: %s", config.ToUrlConn()))
	}
	pid := os.Getpid()
	s.SetConnected(true).SetMessage("Connected successfully").SetPid(pid).SetNewInstance(true)
	instance.SetConn(conn).SetChannel(channel).SetConfig(config).SetState(*s)
	return instance, *s
}

func (c *RabbitMq) Close() {
	c.SetClose(true)
	c.conn.Close()
	c.channel.Close()
}
