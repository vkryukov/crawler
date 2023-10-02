package main

import (
	"bufio"
	"crypto/sha256"
	"database/sql"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync/atomic"
	"syscall"
	"time"
	"unsafe"

	_ "github.com/mattn/go-sqlite3"
)

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

func main() {
	// Process command line arguments
	var dbFile string
	var exclusionFile string
	var logFileName string
	var printInterval int
	var printErrors bool
	var followSymlinks bool
	var retryErrors bool

	flag.StringVar(&dbFile, "db", "index.sqlite", "Path to the SQLite database file")
	flag.StringVar(&exclusionFile, "exclude", "", "Path to the exclusion file")
	flag.StringVar(&logFileName, "log", "errors.log", "Path to the errors log file")
	flag.BoolVar(&printErrors, "print-errors", false, "Print errors to stdout in addition to the log file")
	flag.IntVar(&printInterval, "interval", 1, "Time interval for printing statistics in seconds")
	flag.BoolVar(&followSymlinks, "follow", false, "Follow symbolic links")
	flag.BoolVar(&retryErrors, "retry", false, "Retry files that previously caused errors")
	flag.Parse()

	if len(flag.Args()) < 1 {
		fmt.Println("Usage: program [options] <directory1> [<directory2> ...]")
		flag.PrintDefaults()
		return
	}

	// Initialize logging
	logFileName, err := filepath.Abs(logFileName)
	if err != nil {
		fmt.Println("Error getting absolute path for log file name:", logFileName, err)
		os.Exit(1)
	}
	logFile, err := os.OpenFile(logFileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0666)
	if err != nil {
		fmt.Println("Couldn't open log file:", err)
		os.Exit(1)
	}
	defer func(logFile *os.File) {
		err := logFile.Close()
		if err != nil {
			fmt.Println("Error closing log file:", err)
		}
	}(logFile)

	if printErrors {
		// Log both to the file and stdout
		multiWriter := io.MultiWriter(logFile, os.Stdout)
		log.SetOutput(multiWriter)
	} else {
		// Log only to the file
		log.SetOutput(logFile)
	}

	// Initialize statistics and a mutex for thread-safe access
	stats := &ProcessStats{}

	// Start a goroutine for printing status, unless printInterval is negative
	if printInterval > 0 {
		fmt.Println("Elapsed Time: --:--:--, Files processed: ----, MB processed: ----, Speed: ---- MB/s")
		fmt.Println("Last processed file: ----------------")
		maxWidth, err := getTerminalWidth()
		if err != nil {
			fmt.Println("Error getting terminal width:", err)
			maxWidth = 80
		}

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

				fmt.Printf("\033[2A") // Move cursor 2 lines up
				fmt.Printf("\033[K")  // Clear to the end of line
				fmt.Printf("Elapsed Time: %02d:%02d:%02d, Files processed: %d, MB processed: %.2f, Speed: %.2f MB/s\n", h, m, s, files, float64(bytes)/1e6, speed)
				fmt.Printf("\033[K") // Clear to the end of line
				shortFilename := truncateString(stats.GetLastProcessedFile(), maxWidth-21)
				fmt.Println("Last processed file:", shortFilename)
			}
		}()
	}

	visitedSymlinks := make(map[string]struct{})

	// Initialize database
	dbFile, err = filepath.Abs(dbFile)
	if err != nil {
		log.Println("Error getting absolute path for database file:", dbFile, err)
		os.Exit(1)
	}
	db, err := sql.Open("sqlite3", dbFile)
	if err != nil {
		log.Println("Error opening database:", err)
		os.Exit(1)
	}
	defer func(db *sql.DB) {
		err := db.Close()
		if err != nil {
			log.Println("Error closing database:", err)
		}
	}(db)
	err = createSchema(db)
	if err != nil {
		log.Println("Error creating schema:", err)
		os.Exit(1)
	}

	// Initialize exclusion patterns slice
	var excludePatterns []string
	if exclusionFile != "" {
		excludePatterns = readExcludePatterns(exclusionFile)
	}

	excludePatterns = append(excludePatterns, dbFile)
	excludePatterns = append(excludePatterns, logFileName)

	// Process each directory
	for _, root := range flag.Args() {
		err := processDirectory(root, db, stats, excludePatterns, followSymlinks, visitedSymlinks, retryErrors)
		if err != nil {
			fmt.Printf("Error processing directory %s: %v\n", root, err)
		}
	}
}

func truncateString(str string, num int) string {
	if len(str) > num {
		return str[0:num-3] + "..."
	}
	return str
}
func getTerminalWidth() (int, error) {
	ws := &struct {
		Row    uint16
		Col    uint16
		Xpixel uint16
		Ypixel uint16
	}{}

	retCode, _, errno := syscall.Syscall(syscall.SYS_IOCTL,
		uintptr(syscall.Stdout),
		uintptr(syscall.TIOCGWINSZ),
		uintptr(unsafe.Pointer(ws)))

	if int(retCode) == -1 {
		return 0, errno
	}
	return int(ws.Col), nil
}

// readExcludePatterns reads the exclude file and returns a slice of patterns
func readExcludePatterns(filename string) []string {
	file, err := os.Open(filename)
	if err != nil {
		log.Println("Warning: Could not open exclude file,", err)
		return nil
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("Error closing exclude file:", err)
		}
	}(file)

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
func processDirectory(root string, db *sql.DB, stats *ProcessStats, excludePatterns []string, followSymlinks bool, visitedSymlinks map[string]struct{}, retryErrors bool) error {
	root, err := filepath.Abs(root)
	if err != nil {
		log.Println("Error getting absolute path for root:", root, err)
		return err
	}

	writeError := func(path string, msg string, err error) {
		_, err = db.Exec(`
		INSERT OR REPLACE INTO files(path, error)
		VALUES (?, ?)
		`, path, fmt.Sprintf("%s: %s", msg, err))
		if err != nil {
			log.Println("Error inserting into database:", err)
		}
	}

	var walkFn func(string, os.FileInfo, error) error

	walkFn = func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Println("Error walking file:", err)
			return nil
		}

		// skip the FIFO
		if info.Mode()&os.ModeNamedPipe != 0 {
			writeError(path, "FIFO", nil)
			return nil
		}

		// Skip files that previously caused errors
		if !retryErrors {
			var storedError string
			err = db.QueryRow("SELECT error FROM files WHERE path=? AND error IS NOT NULL", path).Scan(&storedError)
			if err == nil {
				return nil
			}
		}

		// Get file metadata
		fileType, fileSize, creationTime, modificationTime, isDir, isSymlink, target, err := getFileInfo(path, info, followSymlinks)
		if err != nil {
			writeError(path, "getting file info", err)
			return nil
		}
		folderId, err := getFolderID(db, filepath.Dir(path))
		if err != nil {
			writeError(path, "getting folder ID", err)
			return nil
		}

		if match, pattern := isExcluded(path, excludePatterns); match {
			_, err = db.Exec(`
			INSERT OR REPLACE INTO files(path, type, creation_time, modification_time, size, skipped, dir, symlink, exclusion_pattern, folder_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
			`, path, fileType, creationTime, modificationTime, fileSize, 1, isDir, isSymlink, pattern, folderId)
			if err != nil {
				log.Println("Error inserting into database:", err)
			}
			if isDir {
				return filepath.SkipDir
			}
			return nil
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
		stats.SetLastProcessedFile(path)

		// Check if file already exists in database
		var storedModTime string
		err = db.QueryRow("SELECT modification_time FROM files WHERE path=?", path).Scan(&storedModTime)
		if err == nil && storedModTime == modificationTime {
			return nil
		}

		file, err := os.Open(target)
		if err != nil {
			writeError(path, "opening file", err)
			return nil
		}
		defer func(file *os.File) {
			err := file.Close()
			if err != nil {
				log.Println("Error closing file:", err)
			}
		}(file)

		hash := sha256.New()
		_, err = io.Copy(hash, file)
		if err != nil {
			writeError(path, "hashing file", err)
			return nil
		}
		hashValue := fmt.Sprintf("%x", hash.Sum(nil))

		if !isSymlink {
			target = ""
		}
		_, err = db.Exec(`
			INSERT OR REPLACE INTO files(path, type, creation_time, modification_time, hash, size, symlink, followed_symlink, target, folder_id)
			VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		`, path, fileType, creationTime, modificationTime, hashValue, fileSize, isSymlink, followSymlinks, target, folderId)
		if err != nil {
			log.Println("Error inserting into database:", err)
		}
		return nil
	}

	return filepath.Walk(root, walkFn)
}

// getFolderID returns the ID of the folder with the given path, or creates a new folder and returns its ID
func getFolderID(db *sql.DB, path string) (int64, error) {
	var id int64
	err := db.QueryRow("SELECT id FROM folders WHERE path=?", path).Scan(&id)
	if err == nil {
		return id, nil
	}

	if !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}

	res, err := db.Exec("INSERT INTO folders(path) VALUES (?)", path)
	if err != nil {
		return 0, err
	}
	return res.LastInsertId()
}

func createSchema(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS files (
		path TEXT PRIMARY KEY,
		type TEXT,
		creation_time TEXT,
		modification_time TEXT,
		hash TEXT,
		size INTEGER,
		skipped INTEGER DEFAULT 0,
		dir INTEGER DEFAULT 0,
		symlink INTEGER DEFAULT 0,
		followed_symlink INTEGER DEFAULT 0,
		target TEXT DEFAULT NULL,
		exclusion_pattern TEXT DEFAULT NULL,
		error TEXT DEFAULT NULL,
		folder_id INTEGER DEFAULT NULL REFERENCES folders(id)
	);

	CREATE INDEX IF NOT EXISTS hash_idx ON files(hash);

	CREATE TABLE IF NOT EXISTS folders (
		id INTEGER PRIMARY KEY,	    		
	    path TEXT UNIQUE,
	    parent_id INTEGER DEFAULT NULL
	);


	`)
	return err
}

// isExcluded checks if the path matches any of the exclusion patterns, and returns true if it does along with the matching pattern
func isExcluded(path string, excludePatterns []string) (bool, string) {
	for _, pattern := range excludePatterns {
		matched := filepathMatch(pattern, path)
		if matched {
			return matched, pattern
		}
	}
	return false, ""
}

func filepathMatch(pattern, filePath string) bool {
	// Case 1: Simple pattern, e.g., "*.txt"
	if !strings.Contains(pattern, "/") {
		match, _ := path.Match(pattern, filepath.Base(filePath))
		return match
	}

	filePathComponents := strings.Split(filePath, "/")
	if filePathComponents[0] == "" {
		filePathComponents = filePathComponents[1:]
	}
	patternComponents := strings.Split(pattern, "/")

	// Case 2: Pattern starts with a slash, e.g., "/tmp/*"
	if patternComponents[0] == "" {
		patternComponents = patternComponents[1:]
		return fileComponentsMatch(patternComponents, filePathComponents)
	}

	// Case 3: everything else
	for i := 0; i <= len(filePathComponents)-len(patternComponents); i++ {
		if fileComponentsMatch(patternComponents, filePathComponents[i:]) {
			return true
		}
	}

	return false
}

func fileComponentsMatch(patternComponents, filePathComponents []string) bool {
	if len(filePathComponents) < len(patternComponents) {
		return false
	}
	for i := range patternComponents {
		if matched, _ := path.Match(patternComponents[i], filePathComponents[i]); !matched {
			return false
		}
	}
	return true
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
