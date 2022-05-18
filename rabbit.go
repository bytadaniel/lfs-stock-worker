package main

import (
	// "fmt"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/streadway/amqp"
)

type RabbitWrapper struct {
	connection *amqp.Connection
	channel    *amqp.Channel
}

func NewRabbitWrapper(connectionUrl string) *RabbitWrapper {
	var err error
	rabbit := &RabbitWrapper{}
	rabbit.connection, err = amqp.Dial(connectionUrl)
	failOnErr(err, "create channel failed")
	rabbit.channel, err = rabbit.connection.Channel()
	failOnErr(err, "create channel failed")

	rabbit.channel.Qos(50, 0, false)
	return rabbit
}

func (rabbit *RabbitWrapper) AssertQueue(queueName string) {
	fmt.Printf("queue %s\n", queueName)
	q, err := rabbit.channel.QueueDeclare(
		queueName,
		true,
		false,
		false,
		false,
		amqp.Table{
			"x-max-priority": 5,
		},
	)
	failOnErr(err, "Failed to declare a queue")

	log.Println(q)
}

func (rabbit *RabbitWrapper) Consume(queueName string) <-chan amqp.Delivery {
	log.Printf("[RabbitWrapper] Start consuming")

	autoAck := false
	exclusive := false
	consumer := fmt.Sprint(os.Getegid())
	noLocal := false
	noWait := false

	deliveryChannel, err := rabbit.channel.Consume(
		queueName,
		consumer,
		autoAck,
		exclusive,
		noLocal,
		noWait,
		nil,
	)

	failOnErr(err, "Failed on consumption")
	return deliveryChannel
}

func (rabbit *RabbitWrapper) SendMessage(queueName string, taskType string, data MessageData) {
	var err error

	message := RabbitWrapperMessage{
		Type:     	taskType,
		Attempts: 	0,
		Data: 		data,
	}

	log.Println(message.Data)

	body, err := json.Marshal(message)
	failOnErr(err, "Failed on create byte message")

	err = rabbit.channel.Publish(
		"",        // exchange
		queueName, // routing key
		false,     // mandatory
		false,     // immediate
		amqp.Publishing{
			ContentType: "application/json",
			Body:        []byte(body),
		})

	failOnErr(err, "Failed on sending message")
}
