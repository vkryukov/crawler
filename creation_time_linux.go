//go:build linux

package main

import (
	"os"
	"syscall"
	"time"
)

func getCreationTime(info os.FileInfo) string {
	if statT, ok := info.Sys().(*syscall.Stat_t); ok {
		return time.Unix(statT.Ctim.Sec, statT.Ctim.Nsec).Format(time.RFC3339)
	}
	return info.ModTime().Format(time.RFC3339)
}
