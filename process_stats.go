package main

import (
	"fmt"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"
)

// ProcessStats holds processing statistics
type ProcessStats struct {
	FilesProcessed    int64
	BytesProcessed    int64
	lastProcessedFile atomic.Value // Stores string
	printed           bool         // Default false
}

// NewProcessStats creates a new ProcessStats object
func NewProcessStats() *ProcessStats {
	stats := &ProcessStats{}
	stats.lastProcessedFile.Store("")
	return stats
}

func (stats *ProcessStats) Update(path string, fileSize int64) {
	atomic.AddInt64(&stats.FilesProcessed, 1)
	atomic.AddInt64(&stats.BytesProcessed, fileSize)
	stats.lastProcessedFile.Store(path)
}

func (stats *ProcessStats) Print(startTime time.Time) {
	files := atomic.LoadInt64(&stats.FilesProcessed)
	bytes := atomic.LoadInt64(&stats.BytesProcessed)

	elapsed := time.Since(startTime)
	h := int(elapsed.Hours())
	m := int(elapsed.Minutes()) % 60
	s := int(elapsed.Seconds()) % 60
	speed := float64(bytes) / elapsed.Seconds() / 1e6 // in MB/s

	if stats.printed { // Move cursor 2 lines up
		fmt.Printf("\033[2A")
		fmt.Printf("\033[K") // Clear to the end of line
	}
	stats.printed = true

	fmt.Printf("Time: %02d:%02d:%02d, Files: %d, MB: %.2f, Speed: %.2f MB/s\n", h, m, s, files, float64(bytes)/1e6, speed)
	fmt.Printf("\033[K") // Clear to the end of line
	shortFilename := truncateString(stats.lastProcessedFile.Load().(string), getTerminalWidth()-21)
	fmt.Println("Last processed file:", shortFilename)
}

func truncateString(str string, num int) string {
	if len(str) > num {
		return str[0:num-3] + "..."
	}
	return str
}
func getTerminalWidth() int {
	ws := &struct {
		Row    uint16
		Col    uint16
		XPixel uint16
		YPixel uint16
	}{}

	retCode, _, _ := syscall.Syscall(syscall.SYS_IOCTL,
		uintptr(syscall.Stdout),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)))

	if int(retCode) == -1 {
		return 80 // Default value
	}
	return int(ws.Col)
}
