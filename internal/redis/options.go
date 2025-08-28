package redis

type Role string

const (
	RoleMaster Role = "master"
	RoleSlave  Role = "slave"
)

type Options struct {
	Role Role
}
