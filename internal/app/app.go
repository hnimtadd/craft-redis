package app

import (
	"github.com/codecrafters-io/redis-starter-go/internal/app/server"
	"github.com/codecrafters-io/redis-starter-go/internal/redis"
	"github.com/codecrafters-io/redis-starter-go/internal/redis/state/replication"
	"github.com/sirupsen/logrus"
)

type App struct {
	config Config
	logger *logrus.Logger
}

func New() (*App, error) {
	config, err := parseConfig()
	if err != nil {
		return nil, err
	}
	logger := logrus.New()
	if config.Debug {
		logger.SetLevel(logrus.DebugLevel)
	} else {
		logger.SetLevel(logrus.InfoLevel)
	}

	return &App{
		config: config,
		logger: logger,
	}, nil
}

func (a *App) initController() (*redis.Controller, error) {
	opts := redis.Options{}
	if a.config.ReplicaOf != nil {
		// By default, a Redis server assumes the "master" role. When --replicaof
		// flag is passed, the server assumes the "slave" role instead.
		opts.Role = replication.RoleSlave
		opts.MasterHost = a.config.ReplicaOf.MasterHost
		opts.MasterPort = a.config.ReplicaOf.MasterPort
		opts.SlaveHost = "localhost"
		opts.SlavePort = a.config.Port
	} else {
		opts.Role = replication.RoleMaster
		opts.MasterHost = "localhost"
		opts.MasterPort = a.config.Port
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
	controller, err := a.initController()
	if err != nil {
		a.logger.Fatalln("failed to create redis controller", err)
	}

	server, err := a.initServer(controller)
	if err != nil {
		a.logger.Fatalln("failed to create redis server", err)
	}
	a.logger.Info("Server initialized")

	if err := server.ListenAndServe(); err != nil {
		a.logger.Fatalln("stopped listener", err)
	}
}
