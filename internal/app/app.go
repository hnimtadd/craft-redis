package app

import (
	"flag"
)

// Config holds application config
type Config struct {
	Port int
}

func ParseConfig() Config {
	var port int
	flag.IntVar(&port, "port", 6379, "port that redis will listen on, (default: 6379)")
	flag.Parse()
	return Config{
		Port: port,
	}
}
