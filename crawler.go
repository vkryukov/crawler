package main

import (
	"database/sql"
	"flag"
	"fmt"
	_ "github.com/mattn/go-sqlite3"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"
)

func main() {
	// Process command line arguments
	var dbFile string
	var exclusionFile string
	var logFileName string
	var printInterval int
	var printErrors bool
	var retryErrors bool
	var extraLogging bool

	flag.StringVar(&dbFile, "db", "index.sqlite", "Path to the SQLite database file")
	flag.StringVar(&exclusionFile, "exclude", "", "Path to the exclusion file")
	flag.StringVar(&logFileName, "log", "errors.log", "Path to the errors log file")
	flag.BoolVar(&printErrors, "print-errors", false, "Print errors to stdout in addition to the log file")
	flag.IntVar(&printInterval, "interval", 1, "Time interval for printing statistics in seconds")
	flag.BoolVar(&retryErrors, "retry", false, "Retry files that previously caused errors")
	flag.BoolVar(&extraLogging, "extra-logging", false, "Log extra information such as file read and hash generation speed")
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

	// Start a goroutine for printing status, unless printInterval is negative
	stats := NewProcessStats()
	if printInterval > 0 {
		go func() {
			ticker := time.NewTicker(time.Second * time.Duration(printInterval))
			startTime := time.Now()
			stats.Print(startTime)
			for range ticker.C {
				stats.Print(startTime)
			}
		}()
	}

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
		err := processDirectory(root, db, stats, excludePatterns, retryErrors, extraLogging)
		if err != nil {
			fmt.Printf("Error processing directory %s: %v\n", root, err)
		}
	}
}

// processDirectory walks the directory tree and processes each file
func processDirectory(root string, db *sql.DB, stats *ProcessStats, excludePatterns []string, retryErrors bool, extraLogging bool) error {
	root, err := filepath.Abs(root)
	if err != nil {
		log.Println("Error getting absolute path for root:", root, err)
		return err
	}

	return filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		f := NewFileInfo(path, d)

		if err != nil {
			f.WriteError("walking file:", err, db)
			return nil
		}

		// Skip files that previously caused errors
		if !retryErrors {
			var storedError string
			err = db.QueryRow(
				"SELECT error FROM files WHERE path=? AND error IS NOT NULL",
				path).Scan(&storedError)
			if err == nil {
				return nil
			}
		}

		if f.UpdateFolderId(db) != nil || f.UpdateInfo(db) != nil {
			return nil
		}

		// skip the FIFO
		if f.isFifo {
			f.WriteError("FIFO", nil, db)
			return nil
		}

		if match, pattern := isExcluded(path, excludePatterns); match {
			f.ExclusionPattern = sql.NullString{String: pattern, Valid: true}
			f.WriteToDatabase(db)
			return nil
		}

		if f.Dir || f.Symlink.String != "" {
			f.WriteToDatabase(db)
			return nil
		}

		// Update statistics
		stats.Update(path, f.Size)

		// Check if file already exists in database
		var storedModTime string
		err = db.QueryRow("SELECT modification_time FROM files WHERE path=?", path).Scan(&storedModTime)
		if extraLogging {
			log.Println("Path: ", f.Path.String, "stored mod time: ", storedModTime, "new mod time: ", f.ModificationTime.String)
		}
		if err == nil && storedModTime == f.ModificationTime.String {
			return nil
		}

		if f.UpdateHash(db, extraLogging) != nil {
			return nil
		}
		f.WriteToDatabase(db)
		return nil
	})
}
