package utils

import "strings"

func Assert(cond bool, msg ...string) {
	if !cond {
		panic(strings.Join(msg, "\n"))
	}
}
