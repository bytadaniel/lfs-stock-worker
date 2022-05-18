package main

import (
	"fmt"
	"os"
)

// "log"

// "github.com/streadway/amqp"

func main () {
  fmt.Printf("pid: %d\n", os.Getpid())

  consumptionQueue := "parse_stocks"
  rabbit := NewRabbitWrapper("amqp://guest:guest@localhost:5672")

  worker := NewWorker(consumptionQueue, rabbit)
  worker.ConnectRabbit()
  worker.Listen()
}