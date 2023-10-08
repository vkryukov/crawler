package main

import (
	"crypto/sha256"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"time"
)

func createSchema(db *sql.DB) error {
	_, err := db.Exec(`
	CREATE TABLE IF NOT EXISTS files (
		path TEXT PRIMARY KEY,
		name TEXT,
		type TEXT,
		creation_time TEXT,
		modification_time TEXT,
		hash TEXT,
		size INTEGER,
		dir INTEGER DEFAULT 0,
		symlink TEXT DEFAULT '',
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

type FileInfo struct {
	d                fs.DirEntry
	Path             sql.NullString
	Name             sql.NullString
	Type             sql.NullString
	CreationTime     sql.NullString
	ModificationTime sql.NullString
	Hash             sql.NullString
	Size             int64
	Dir              bool
	Symlink          sql.NullString
	ExclusionPattern sql.NullString
	Error            sql.NullString
	FolderId         int64
	isFifo           bool
}

func NewFileInfo(path string, d fs.DirEntry) *FileInfo {
	info := &FileInfo{}
	info.d = d
	info.Path = sql.NullString{String: path, Valid: true}
	info.Name = sql.NullString{String: d.Name(), Valid: true}
	info.Type = sql.NullString{String: filepath.Ext(path), Valid: true}
	info.Dir = d.IsDir()
	return info
}

func (f *FileInfo) WriteToDatabase(db *sql.DB) {
	_, err := db.Exec(`
	INSERT OR REPLACE INTO files(path, name, type, creation_time, modification_time, hash, size, dir, symlink, 
	                             exclusion_pattern, error, folder_id)
	VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
	`, f.Path, f.Name, f.Type, f.CreationTime, f.ModificationTime, f.Hash, f.Size, f.Dir, f.Symlink,
		f.ExclusionPattern, f.Error, f.FolderId)
	if err != nil {
		log.Fatalln("Error inserting into database:", err)
	}
}

func (f *FileInfo) WriteError(msg string, err error, db *sql.DB) {
	f.Error = sql.NullString{String: fmt.Sprintf("%s: %s", msg, err), Valid: true}
	f.WriteToDatabase(db)
}

func (f *FileInfo) UpdateFolderId(db *sql.DB) error {
	var err error
	f.FolderId, err = getFolderID(db, filepath.Dir(f.Path.String))
	if err != nil {
		f.WriteError("getting folder ID", err, db)
	}
	return err
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

	if path == "/" {
		res, err := db.Exec("INSERT INTO folders(path) VALUES (?)", path)
		if err != nil {
			return 0, err
		}
		return res.LastInsertId()
	} else {
		parentId, err := getFolderID(db, filepath.Dir(path))
		if err != nil {
			return 0, err
		}
		res, err := db.Exec("INSERT INTO folders(path, parent_id) VALUES (?, ?)", path, parentId)
		if err != nil {
			return 0, err
		}
		return res.LastInsertId()
	}
}

func (f *FileInfo) UpdateInfo(db *sql.DB) error {
	info, err := f.d.Info()
	if err != nil {
		f.WriteError("getting file info", err, db)
	} else {
		f.CreationTime = sql.NullString{String: getCreationTime(info), Valid: true}
		f.ModificationTime = sql.NullString{String: info.ModTime().Format(time.RFC3339), Valid: true}
		f.Size = info.Size()
		f.isFifo = info.Mode()&os.ModeNamedPipe != 0
		if info.Mode()&os.ModeSymlink != 0 {
			var symlink string
			symlink, err = os.Readlink(f.Path.String)
			if err != nil {
				f.WriteError("reading symlink", err, db)
			} else {
				f.Symlink = sql.NullString{String: symlink, Valid: true}
			}
		}
	}
	return err
}

func (f *FileInfo) UpdateHash(db *sql.DB, extraLogging bool) error {
	file, err := os.Open(f.Path.String)
	if err != nil {
		f.WriteError("opening file", err, db)
		return err
	}
	defer func(file *os.File) {
		err := file.Close()
		if err != nil {
			log.Println("Error closing file:", err)
		}
	}(file)

	sizeMb := float64(f.Size) / (1024 * 1024)

	if extraLogging {
		readStart := time.Now()
		_, err = io.Copy(io.Discard, file)
		if err != nil {
			f.WriteError("reading file", err, db)
			return err
		}
		readDuration := time.Since(readStart)
		readSpeed := sizeMb / readDuration.Seconds() // MB/s
		log.Printf("Read speed for %s [%.2f MB]: %.2f MB/s\n", f.Path.String, sizeMb, readSpeed)

		// Reset file pointer to the beginning
		_, err = file.Seek(0, 0)
		if err != nil {
			f.WriteError("seeking file", err, db)
			return err
		}
	}

	hashStart := time.Now()
	hash := sha256.New()
	_, err = io.Copy(hash, file)
	if err != nil {
		f.WriteError("hashing file", err, db)
		return err
	}
	if extraLogging {
		hashDuration := time.Since(hashStart)
		hashSpeed := sizeMb / hashDuration.Seconds() // MB/s
		log.Printf("Hash speed for %s [%.2f MB]: %.2f MB/s\n", f.Path.String, sizeMb, hashSpeed)
	}
	f.Hash = sql.NullString{String: fmt.Sprintf("%x", hash.Sum(nil)), Valid: true}
	return nil
}
