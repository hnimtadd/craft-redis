package utils

import "strings"

func Assert(cond bool, msg ...string) {
	if !cond {
		panic(strings.Join(msg, "\n"))
	}
}

func InstanceOf[T any](val any) bool {
	_, isInstanceOf := val.(T)
	return isInstanceOf
}
