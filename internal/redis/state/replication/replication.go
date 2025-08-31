package replication

import "net"

type Role string

const (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
)

type Replica struct {
	Config  Config
	Conn    net.Conn
	IsReady bool
}

type Config struct {
	ListeningPort int
}
