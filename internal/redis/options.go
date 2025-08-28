package redis

import (
	"fmt"
	"strings"
)

type Role string

const (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
)

type Options struct {
	Role Role
}

func (o Options) String() string {
	builder := new(strings.Builder)
	fmt.Fprintf(builder, "Role: %v\n", o.Role)
	return builder.String()
}
