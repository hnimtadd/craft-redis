package main

import (
	"log"
	"os"

	"github.com/codecrafters-io/redis-starter-go/internal/app"
)

func main() {
	app, err := app.New()
	if err != nil {
		log.Println("failed to start application", err)
		os.Exit(1)
	}
	app.Start()
}
