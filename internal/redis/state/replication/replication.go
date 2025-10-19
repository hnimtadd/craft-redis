package replication

import (
	"github.com/codecrafters-io/redis-starter-go/internal/network"
)

type Role string

const (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
)

type Replica struct {
	Config  Config
	Conn    network.Connection
	IsReady bool
}

type Config struct {
	ListeningPort int
}
