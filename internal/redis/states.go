package redis

import "github.com/codecrafters-io/redis-starter-go/internal/redis/state/replication"

type ReplicationState struct {
	Role             replication.Role
	MasterReplID     string
	MasterReplOffset int

	replicas *Set[replication.Replica]
}
