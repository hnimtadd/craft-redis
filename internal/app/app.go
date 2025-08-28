package app

import (
	"github.com/codecrafters-io/redis-starter-go/internal/app/server"
	"github.com/codecrafters-io/redis-starter-go/internal/redis"
)

type App struct {
	server     *server.Server
	controller *redis.Controller
	config     Config
}

func New() *App {
	config := parseConfig()
	controllerOpts := redis.Options{
		Role: redis.RoleMaster,
	}
	controller := redis.NewController(controllerOpts)
	opts := server.Options{
		Port: config.Port,
	}
	server := server.NewServer(controller, opts)
	return &App{
		config:     config,
		controller: controller,
		server:     server,
	}
}

func (a *App) Start() {
	panic(a.server.ListenAndServe())
}
