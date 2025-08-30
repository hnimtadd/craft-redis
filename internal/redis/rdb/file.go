package rdb

import (
	"encoding/hex"
	"fmt"

	"github.com/codecrafters-io/redis-starter-go/internal/redis/resp"
)

type File struct {
	Content []byte
}

func (f File) String() string {
	return fmt.Sprintf("$%d%s%s", len(f.Content), resp.Terminator, f.Content)
}

var EmptyFile = func() File {
	emptyHexHash := "524544495330303131fa0972656469732d76657205372e322e30fa0a72656469732d62697473c040fa056374696d65c26d08bc65fa08757365642d6d656dc2b0c41000fa08616f662d62617365c000fff06e3bfec0ff5aa2"
	value, _ := hex.DecodeString(emptyHexHash)
	return File{Content: value}
}()
