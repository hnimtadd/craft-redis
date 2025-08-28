package redis

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/redis/state/replication"
)

type Options struct {
	Role replication.Role
	// MasterHost, Port indicates port of master node, incase this instance
	// is a slave.
	MasterHost string
	MasterPort int
	// If current node is slave, it is its running port.
	SlaveHost string
	SlavePort int
}

func (o Options) String() string {
	builder := new(strings.Builder)
	fmt.Fprintf(builder, "Role: %v\n", o.Role)
	fmt.Fprintf(builder, "Master: %v:%v\n", o.MasterHost, o.MasterPort)
	fmt.Fprintf(builder, "Slave: %v:%v\n", o.SlaveHost, o.SlavePort)
	return builder.String()
}
