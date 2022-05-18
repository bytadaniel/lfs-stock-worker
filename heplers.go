package main

import (
  "log"
  "fmt"
)

func failOnErr(err error, message string) {
	if err != nil {
    log.Fatalf("%s:%s\n", message, err)
    panic(fmt.Sprintf("%s:%s", message, err))
	}
}