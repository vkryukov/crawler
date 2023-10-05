package main

import "sync/atomic"

// ProcessStats holds processing statistics
type ProcessStats struct {
	FilesProcessed    int64
	BytesProcessed    int64
	lastProcessedFile atomic.Value // Stores string
}

func (stats *ProcessStats) SetLastProcessedFile(fileName string) {
	stats.lastProcessedFile.Store(fileName)
}

func (stats *ProcessStats) GetLastProcessedFile() string {
	return stats.lastProcessedFile.Load().(string)
}
