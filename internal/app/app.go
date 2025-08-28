package app

import (
	"github.com/codecrafters-io/redis-starter-go/internal/app/server"
	"github.com/codecrafters-io/redis-starter-go/internal/redis"
	"github.com/sirupsen/logrus"
)

type App struct {
	config Config
	logger *logrus.Logger
}

func New() *App {
	config := parseConfig()
	logger := logrus.New()
	if config.Debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	return &App{
		config: config,
		logger: logger,
	}
}

func (a *App) initController() (*redis.Controller, error) {
	opts := redis.Options{Role: redis.RoleMaster}
	if a.config.ReplicaOf != "" {
		// By default, a Redis server assumes the "master" role. When --replicaof
		// flag is passed, the server assumes the "slave" role instead.
		opts.Role = redis.RoleSlave
	}

	a.logger.Debug("init redis controller with config\n", opts)
	controller := redis.NewController(opts)
	return controller, nil
}

func (a *App) initServer(controller *redis.Controller) (*server.Server, error) {
	opts := server.Options{
		Port: a.config.Port,
	}
	server := server.NewServer(controller, opts)
	return server, nil
}

func (a *App) Start() {
	a.logger.Debug("start the server with config\n", a.config)
	controller, err := a.initController()
	if err != nil {
		a.logger.Fatalln("failed to create redis controller", err)
	}
	server, err := a.initServer(controller)
	if err != nil {
		a.logger.Fatalln("failed to create redis server", err)
	}

	if err := server.ListenAndServe(); err != nil {
		a.logger.Fatalln("stopped listener", err)
	}
}
