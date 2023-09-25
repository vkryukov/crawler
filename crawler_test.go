package main

import (
	"testing"
)

func TestFilepathMatch(t *testing.T) {
	testCases := []struct {
		pattern  string
		path     string
		expected bool
	}{
		{"*.txt", "file.txt", true},                // Test matching a simple wildcard pattern
		{"*.txt", "file.jpg", false},               // Test not matching any pattern
		{"*.txt", "/tmp/file.txt", true},           // Test matching an absolute path pattern
		{"logs/*.txt", "logs/file.txt", true},      // Test matching a relative multi-folder pattern
		{"logs/*.txt", "logs/file.jpg", false},     // Test not matching any pattern with a subfolder
		{"logs/*.txt", "logs/a/b/cde", false},      // Test not matching any pattern with a subfolder
		{"logs/*.txt", "/tmp/logs/file.txt", true}, // Test matching multiple patterns
		{"logs/*.txt", "/a/b/c/d", false},          // Test not matching any pattern with a subfolder
		{"logs/*.txt", "/root/logs/a/b", false},    // Test matching a relative multi-folder pattern
		{"/a/*/b/*", "/a/x/b/d", true},             // Test matching a relative multi-folder pattern
		{"/a/*/b/*", "/a/b/c/d", false},            // Test not matching any pattern with a subfolder
		{"/logs/*.txt", "/a/logs/file.txt", false}, // Test not matching any pattern with a subfolder
	}

	for _, tc := range testCases {
		if matched := filepathMatch(tc.pattern, tc.path); matched != tc.expected {
			t.Errorf("filepathMatch(%q, %q) = %v, want %v", tc.pattern, tc.path, matched, tc.expected)
		}
	}
}

func TestIsExcluded(t *testing.T) {
	excludePatterns := []string{"/a/*/b/*", "*.txt", "/tmp/*", "logs/*.txt"}

	testCases := []struct {
		path     string
		expected bool
	}{
		{"/a/x/b/d", true},           // Test matching a relative multi-folder pattern
		{"file.jpg", false},          // Test not matching any pattern
		{"file.txt", true},           // Test matching a simple wildcard pattern
		{"/tmp/file.txt", true},      // Test matching an absolute path pattern
		{"logs/file.txt", true},      // Test matching a relative multi-folder pattern
		{"logs/file.jpg", false},     // Test not matching any pattern with a subfolder
		{"logs/a/b/cde", false},      // Test not matching any pattern with a subfolder
		{"/tmp/logs/file.txt", true}, // Test matching multiple patterns
		{"/a/b/c/d", false},          // Test not matching any pattern with a subfolder
		{"/root/logs/a/b", false},    // Test matching a relative multi-folder pattern
	}

	for _, tc := range testCases {
		if matched, _ := isExcluded(tc.path, excludePatterns); matched != tc.expected {
			t.Errorf("isExcluded(%q, %q) = %v, want %v", tc.path, excludePatterns, matched, tc.expected)
		}
	}
}
