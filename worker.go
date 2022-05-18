package main

import (
	"encoding/json"
	"log"
)

type Worker struct {
	ConsumeQueue string
	Rabbit       *RabbitWrapper
	Handlers     map[string]func(MessageData)
}

func NewWorker(consumeQueue string, rabbit *RabbitWrapper) *Worker {
	worker := new(Worker)
	worker.ConsumeQueue = consumeQueue
	worker.Rabbit = rabbit
	worker.Handlers = map[string]func(MessageData){
		"parse_new_stocks_for_article": func(messageData MessageData) {
			ParseStocksHandler(
				*worker.Rabbit,
				messageData,
			)
		},
	}
	return worker
}

func (worker *Worker) ConnectRabbit() {
	log.Printf("[Worker] Connect to rabbit")
	worker.Rabbit.AssertQueue(worker.ConsumeQueue)
}

func (worker *Worker) ProcessMessage(message RabbitWrapperMessage) {
	worker.ProcessTask(message.Type, message.Data)
}

func (worker *Worker) ProcessTask(mt string, md MessageData) {
	handler := worker.Handlers[mt]
	if handler != nil {
		log.Println("Before handle task")
		handler(md)
	} else {
		log.Println("Unknown task type: ", mt)
	}
}

func (worker *Worker) Listen() {
	worker.ConnectRabbit()
	messageChannel := worker.Rabbit.Consume(worker.ConsumeQueue)
	forever := make(chan bool)
	// run consumption in goroutine
	go func() {
		for data := range messageChannel {
			message := RabbitWrapperMessage{}
			json.Unmarshal(data.Body, &message)

			log.Printf("Received Rabbit message: %v\n", message)
			worker.ProcessMessage(message)
			worker.Rabbit.channel.Ack(data.DeliveryTag, false)
		}
	}()

	log.Printf("[*] Waiting for messagees, To exit press CTRL+C")

	<-forever
}

type RabbitWrapperMessage struct {
	Type 		string 						`json:"type"`
	Attempts 	int    						`json:"attempts"`
	Data		MessageData					`json:"data"`
}

type MessageData struct {
	Articles 	[]string 			`json:"articles"`
	XInfo	 	string				`json:"xInfo"`	
	Table		string				`json:"table"`
	Row			interface{}			`json:"row"`
}