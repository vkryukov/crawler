package main

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

// ProcessStats holds processing statistics
type ProcessStats struct {
	FilesProcessed int64
	BytesProcessed int64
}

func main() {
	// Check for correct number of arguments
	if len(os.Args) < 3 || len(os.Args) > 4 {
		fmt.Println("Usage: program <directory> <database-path> [exclude-file]")
		return
	}

	// Initialize statistics and a mutex for thread-safe access
	stats := &ProcessStats{}
	var mu sync.Mutex

	// Start a goroutine for printing status
	go func() {
		ticker := time.NewTicker(time.Second * 5)
		startTime := time.Now()

		for range ticker.C {
			mu.Lock()
			files := atomic.LoadInt64(&stats.FilesProcessed)
			bytes := atomic.LoadInt64(&stats.BytesProcessed)
			mu.Unlock()

			elapsed := time.Since(startTime)
			h := int(elapsed.Hours())
			m := int(elapsed.Minutes()) % 60
			s := int(elapsed.Seconds()) % 60
			speed := float64(bytes) / elapsed.Seconds() / 1e6 // in MB/s

			fmt.Printf("Elapsed Time: %02d:%02d:%02d, Files processed: %d, MB processed: %.2f, Speed: %.2f MB/s\n", h, m, s, files, float64(bytes)/1e6, speed)
		}
	}()

	// Initialize exclusion patterns slice
	var excludePatterns []string
	if len(os.Args) == 4 {
		excludePatterns = readExcludePatterns(os.Args[3])
	}

	// Open log file
	logFile, err := os.OpenFile("errors.log", os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("Couldn't open log file:", err)
		return
	}
	defer logFile.Close()

	// Log both to the file and stdout
	multiWriter := io.MultiWriter(logFile, os.Stdout)
	log.SetOutput(multiWriter)

	// Process the directory
	err = processDirectory(os.Args[1], os.Args[2], stats, &mu, excludePatterns)
	if err != nil {
		fmt.Println("Error:", err)
	}
}

// reaadExcludePatterns reads the exclude file and returns a slice of patterns
func readExcludePatterns(filename string) []string {
	file, err := os.Open(filename)
	if err != nil {
		log.Println("Warning: Could not open exclude file,", err)
		return nil
	}
	defer file.Close()

	var patterns []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		// Ignore comments and empty lines
		if strings.HasPrefix(line, "#") || line == "" {
			continue
		}
		patterns = append(patterns, line)
	}

	if err := scanner.Err(); err != nil {
		log.Println("Warning: Error reading exclude file,", err)
		return nil
	}
	return patterns
}

// processDirectory walks the directory tree and processes each file
func processDirectory(root string, dbPath string, stats *ProcessStats, mu *sync.Mutex, excludePatterns []string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		log.Println("Error opening database:", err)
		return err
	}
	defer db.Close()

	_, err = db.Exec(`
	CREATE TABLE IF NOT EXISTS filedata (
		filepath TEXT PRIMARY KEY,
		filetype TEXT,
		creation_time TEXT,
		modification_time TEXT,
		hash TEXT,
		filesize INTEGER,
		skipped INTEGER DEFAULT 0,
		is_dir INTEGER DEFAULT 0,
		exclusion_pattern TEXT DEFAULT NULL
	);
	`)
	if err != nil {
		log.Println("Error creating table:", err)
		return err
	}

	return filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		// Get file metadata
		fileType := filepath.Ext(path)
		fileSize := info.Size()
		creationTime := getCreationTime(info)
		modificationTime := info.ModTime().Format(time.RFC3339)

		logExclusionPatternToDB := func(pattern string) {
			_, err = db.Exec(`
			INSERT OR REPLACE INTO filedata(filepath, filetype, creation_time, modification_time, filesize, skipped, is_dir, exclusion_pattern)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?)
		`, path, fileType, creationTime, modificationTime, fileSize, 1, info.IsDir(), pattern)
			if err != nil {
				log.Println("Error inserting into database:", err)
			}
		}

		// Check against exclusion patterns
		for _, pattern := range excludePatterns {
			if strings.HasPrefix(pattern, "/") {
				// Treat as an absolute path pattern
				matched, _ := filepath.Match(pattern, path)
				if matched {
					logExclusionPatternToDB(pattern)
					if info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			} else if strings.Contains(pattern, "/") {
				// Treat as a relative multi-folder pattern
				if strings.Contains(path, pattern) {
					logExclusionPatternToDB(pattern)
					if info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			} else {
				// Treat as a simple wildcard match on file or directory name
				matched, _ := filepath.Match(pattern, filepath.Base(path))
				if matched {
					logExclusionPatternToDB(pattern)
					if info.IsDir() {
						return filepath.SkipDir
					}
					return nil
				}
			}
		}

		if err != nil {
			log.Println("Error walking file:", err)
			return nil
		}

		// Check if file is a symbolic link pointing to a directory
		linfo, err := os.Lstat(path)
		if err != nil {
			log.Println("Error getting Lstat:", err)
			return nil
		}
		if linfo.Mode()&os.ModeSymlink != 0 {
			targetPath, err := os.Readlink(path)
			if err != nil {
				log.Println("Error reading symbolic link:", err)
				return nil
			}

			targetInfo, err := os.Stat(targetPath)
			if err != nil {
				log.Println("Error getting Stat of target:", err)
				return nil
			}
			if targetInfo.IsDir() {
				// Handle case when symbolic link points to a directory
				return nil
			}
		}

		if info.IsDir() {
			return nil
		}

		// Update statistics
		mu.Lock()
		atomic.AddInt64(&stats.FilesProcessed, 1)
		atomic.AddInt64(&stats.BytesProcessed, fileSize)
		mu.Unlock()

		// Check if file already exists in database
		var storedModTime string
		err = db.QueryRow("SELECT modification_time FROM filedata WHERE filepath=?", path).Scan(&storedModTime)
		if err == nil && storedModTime == modificationTime {
			return nil
		}

		file, err := os.Open(path)
		if err != nil {
			log.Println("Error opening file:", err)
			return nil
		}
		defer file.Close()

		hash := sha256.New()
		_, err = io.Copy(hash, file)
		if err != nil {
			log.Println("Error hashing file:", err)
			return nil
		}
		hashValue := fmt.Sprintf("%x", hash.Sum(nil))

		_, err = db.Exec(`
			INSERT OR REPLACE INTO filedata(filepath, filetype, creation_time, modification_time, hash, filesize)
			VALUES (?, ?, ?, ?, ?, ?)
		`, path, fileType, creationTime, modificationTime, hashValue, fileSize)
		if err != nil {
			log.Println("Error inserting into database:", err)
		}
		return nil
	})
}
