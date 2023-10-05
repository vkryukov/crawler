package main

import (
	"bufio"
	"log"
	"os"
	"path"
	"path/filepath"
	"strings"
)

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
	// Patterns ending with / match both the directory and its contents
	if strings.HasSuffix(pattern, "/") {
		return filepathMatch(pattern[:len(pattern)-1], filePath) || filepathMatch(pattern+"*", filePath)
	}

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
