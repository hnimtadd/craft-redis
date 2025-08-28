package redis

import (
	"fmt"
	"strings"

	"github.com/codecrafters-io/redis-starter-go/internal/redis/state/replication"
)

type Options struct {
	Role       replication.Role
	MasterHost string
	MasterPort int
}

func (o Options) String() string {
	builder := new(strings.Builder)
	fmt.Fprintf(builder, "Role: %v\n", o.Role)
	return builder.String()
}
