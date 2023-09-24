//go:build darwin

package main

import (
	"os"
	"syscall"
	"time"
)

func getCreationTime(info os.FileInfo) string {
	if statT, ok := info.Sys().(*syscall.Stat_t); ok {
		return time.Unix(statT.Birthtimespec.Sec, statT.Birthtimespec.Nsec).Format(time.RFC3339)
	}
	return info.ModTime().Format(time.RFC3339)
}
