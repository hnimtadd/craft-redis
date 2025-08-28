package app

import (
	"errors"
	"flag"
	"fmt"
	"strconv"
	"strings"
)

// Config holds application config
type Config struct {
	Debug     bool
	Port      int
	ReplicaOf *Replica
}

type Replica struct {
	MasterHost string
	MasterPort int
}

func (c Config) String() string {
	builder := new(strings.Builder)
	fmt.Fprintf(builder, "Debug: %v\n", c.Debug)
	fmt.Fprintf(builder, "Port: %v\n", c.Port)
	fmt.Fprintf(builder, "Replica Of: %v\n", c.ReplicaOf)
	return builder.String()
}

func parseConfig() (Config, error) {
	portPtr := flag.Int("port", 6379, "port that redis will listen on, (default: 6379)")
	replicaOfPtr := flag.String("replicaof", "", "")
	debugPtr := flag.Bool("debug", false, "enable debug log, (default: false)")
	flag.Parse()
	replicaOf, err := parseReplicaOf(*replicaOfPtr)
	if err != nil {
		return Config{}, err
	}
	return Config{
		Port:      *portPtr,
		ReplicaOf: replicaOf,
		Debug:     *debugPtr,
	}, nil
}

func parseReplicaOf(arg string) (*Replica, error) {
	if arg == "" {
		return nil, nil
	}

	parts := strings.Split(arg, " ")
	if len(parts) != 2 {
		return nil, errors.New("invalid replicaof arg")
	}
	host := parts[0]

	port, err := strconv.ParseInt(parts[1], 10, 64)
	if err != nil {
		return nil, errors.New("invalid replicaof arg")
	}
	return &Replica{
		MasterHost: host,
		MasterPort: int(port),
	}, nil
}
