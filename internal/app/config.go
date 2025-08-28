package app

import (
	"flag"
	"fmt"
	"strings"
)

// Config holds application config
type Config struct {
	Debug     bool
	Port      int
	ReplicaOf string
}

func (c Config) String() string {
	builder := new(strings.Builder)
	fmt.Fprintf(builder, "Debug: %v\n", c.Debug)
	fmt.Fprintf(builder, "Port: %v\n", c.Port)
	fmt.Fprintf(builder, "Replica Of: %v\n", c.ReplicaOf)
	return builder.String()
}

func parseConfig() Config {
	portPtr := flag.Int("port", 6379, "port that redis will listen on, (default: 6379)")
	replicaOfPtr := flag.String("replicaof", "", "")
	debugPtr := flag.Bool("debug", false, "enable debug log, (default: false)")
	flag.Parse()
	return Config{
		Port:      *portPtr,
		ReplicaOf: *replicaOfPtr,
		Debug:     *debugPtr,
	}
}
