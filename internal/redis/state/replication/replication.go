package replication

type Role string

const (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
)

type Replication struct {
	Role             Role
	MasterReplID     string
	MasterReplOffset int
}

type Config struct {
	RemoteAddr string
}
