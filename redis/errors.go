package redis

import "errors"

var (
	ErrInvalidArgs error = errors.New("invalid args")
	ErrInvalidCmd  error = errors.New("invalid cmd")
)
