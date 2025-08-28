package main

import (
	"github.com/codecrafters-io/redis-starter-go/internal/app"
	"github.com/codecrafters-io/redis-starter-go/internal/app/server"
	"github.com/codecrafters-io/redis-starter-go/internal/redis"
)

func main() {
	config := app.ParseConfig()
	controller := redis.NewController()

	opts := server.Options{
		Port: config.Port,
	}

	server := server.NewServer(controller, opts)

	panic(server.ListenAndServe())
}
