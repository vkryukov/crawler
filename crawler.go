package main

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
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
	var dbFile string
	var exclusionFile string
	var logFileName string
	var printInterval int
	var followSymlinks bool

	flag.StringVar(&dbFile, "db", "index.sqlite", "Path to the SQLite database file")
	flag.StringVar(&exclusionFile, "exclude", "", "Path to the exclusion file")
	flag.StringVar(&logFileName, "log", "errors.log", "Path to the log file")
	flag.IntVar(&printInterval, "interval", 5, "Time interval for printing statistics in seconds")
	flag.BoolVar(&followSymlinks, "follow", false, "Follow symbolic links")
	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Println("Usage: program [options] <directory1> [<directory2> ...]")
		flag.PrintDefaults()
		return
	}

	// Initialize statistics and a mutex for thread-safe access
	stats := &ProcessStats{}

	// Start a goroutine for printing status, unless printInterval is negative
	if printInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Second * time.Duration(printInterval))
			startTime := time.Now()

			for range ticker.C {
				files := atomic.LoadInt64(&stats.FilesProcessed)
				bytes := atomic.LoadInt64(&stats.BytesProcessed)

				elapsed := time.Since(startTime)
				h := int(elapsed.Hours())
				m := int(elapsed.Minutes()) % 60
				s := int(elapsed.Seconds()) % 60
				speed := float64(bytes) / elapsed.Seconds() / 1e6 // in MB/s

				fmt.Printf("Elapsed Time: %02d:%02d:%02d, Files processed: %d, MB processed: %.2f, Speed: %.2f MB/s\n", h, m, s, files, float64(bytes)/1e6, speed)
			}
		}()
	}

	// Initialize exclusion patterns slice
	var excludePatterns []string
	if exclusionFile != "" {
		excludePatterns = readExcludePatterns(exclusionFile)
	}

	// Open log file
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("Couldn't open log file:", err)
		return
	}
	defer logFile.Close()

	// Log both to the file and stdout
	multiWriter := io.MultiWriter(logFile, os.Stdout)
	log.SetOutput(multiWriter)

	visitedSymlinks := make(map[string]struct{})

	// Process each directory
	for _, root := range flag.Args() {
		err := processDirectory(root, dbFile, logFileName, stats, excludePatterns, followSymlinks, visitedSymlinks)
		if err != nil {
			fmt.Printf("Error processing directory %s: %v\n", root, err)
		}
	}
}

// readExcludePatterns reads the exclude file and returns a slice of patterns
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
func processDirectory(root string, dbPath string, logFileName string, stats *ProcessStats, excludePatterns []string, followSymlinks bool, visitedSymlinks map[string]struct{}) error {
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
		dir INTEGER DEFAULT 0,
		symlink INTEGER DEFAULT 0,
		followed_symlink INTEGER DEFAULT 0,
		target TEXT DEFAULT NULL,
		exclusion_pattern TEXT DEFAULT NULL
	);
	`)
	if err != nil {
		log.Println("Error creating table:", err)
		return err
	}

	var walkFn func(string, os.FileInfo, error) error

	walkFn = func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("Error walking file:", err)
			return nil
		}

		// Never walk the database file or the log file
		absPath, err := filepath.Abs(path)
		if err != nil {
			log.Println("Error getting absolute path for path:", path, err)
			return nil
		}
		absDBPath, err := filepath.Abs(dbPath)
		if err != nil {
			log.Println("Error getting absolute path for database path:", dbPath, err)
			return nil
		}
		absLogFileName, err := filepath.Abs(logFileName)
		if err != nil {
			log.Println("Error getting absolute path for log file name:", logFileName, err)
			return nil
		}
		if absPath == absDBPath || absPath == absLogFileName {
			return nil
		}

		// Get file metadata
		fileType, fileSize, creationTime, modificationTime, isDir, isSymlink, target, err := getFileInfo(path, info, followSymlinks)
		if err != nil {
			log.Println("Error getting file info:", err)
			return nil
		}

		logExclusionPatternToDB := func(pattern string) {
			_, err = db.Exec(`
			INSERT OR REPLACE INTO filedata(filepath, filetype, creation_time, modification_time, filesize, skipped, dir, symlink, exclusion_pattern)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, path, fileType, creationTime, modificationTime, fileSize, 1, isDir, isSymlink, pattern)
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
					if isDir {
						return filepath.SkipDir
					}
					return nil
				}
			} else if strings.Contains(pattern, "/") {
				// Treat as a relative multi-folder pattern
				if strings.Contains(path, pattern) {
					logExclusionPatternToDB(pattern)
					if isDir {
						return filepath.SkipDir
					}
					return nil
				}
			} else {
				// Treat as a simple wildcard match on file or directory name
				matched, _ := filepath.Match(pattern, filepath.Base(path))
				if matched {
					logExclusionPatternToDB(pattern)
					if isDir {
						return filepath.SkipDir
					}
					return nil
				}
			}
		}

		if isDir {
			if isSymlink && followSymlinks {
				if _, alreadyVisited := visitedSymlinks[target]; alreadyVisited {
					log.Println("Symlink loop detected:", path, "->", target)
					return nil
				}
				visitedSymlinks[target] = struct{}{}
				log.Println("Following symlink:", path, "->", target)
				return filepath.Walk(target, walkFn)
			} else {
				return nil
			}
		}

		// Update statistics
		atomic.AddInt64(&stats.FilesProcessed, 1)
		atomic.AddInt64(&stats.BytesProcessed, fileSize)

		// Check if file already exists in database
		var storedModTime string
		err = db.QueryRow("SELECT modification_time FROM filedata WHERE filepath=?", path).Scan(&storedModTime)
		if err == nil && storedModTime == modificationTime {
			return nil
		}

		file, err := os.Open(target)
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

		if !isSymlink {
			target = ""
		}
		_, err = db.Exec(`
			INSERT OR REPLACE INTO filedata(filepath, filetype, creation_time, modification_time, hash, filesize, symlink, followed_symlink, target)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, path, fileType, creationTime, modificationTime, hashValue, fileSize, isSymlink, followSymlinks, target)
		if err != nil {
			log.Println("Error inserting into database:", err)
		}
		return nil
	}

	return filepath.Walk(root, walkFn)
}

func getFileInfo(path string, info os.FileInfo, followSymlinks bool) (string, int64, string, string, bool, bool, string, error) {
	var fileSize int64
	var creationTime string
	var modificationTime string
	var isDir bool
	var isSymlink bool
	var target string
	var err error

	if info.Mode()&os.ModeSymlink != 0 {
		isSymlink = true
		if followSymlinks {
			target, err = os.Readlink(path)
			if err != nil {
				return "", 0, "", "", false, false, "", err
			}
			info, err = os.Stat(target)
			if err != nil {
				return "", 0, "", "", false, false, "", fmt.Errorf("error following symlink in %s: %w", path, err)
			}
		} else {
			target, err = os.Readlink(path)
			if err != nil {
				return "", 0, "", "", false, false, "", err
			}
			info, err = os.Lstat(target)
			if err != nil {
				return "", 0, "", "", false, false, "", fmt.Errorf("error following symlink in %s: %w", path, err)
			}
		}
	} else {
		target = path
	}

	fileType := filepath.Ext(path)
	fileSize = info.Size()
	isDir = info.IsDir()
	creationTime = getCreationTime(info)
	modificationTime = info.ModTime().Format(time.RFC3339)

	return fileType, fileSize, creationTime, modificationTime, isDir, isSymlink, target, nil
}
