package main

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"
)

// --- Unit Tests ---

func TestParseLineCount(t *testing.T) {
	testCases := []struct {
		name     string
		input    string
		expected int64
		hasError bool
	}{
		{"Simple Number", "12345", 12345, false},
		{"Number with Commas", "1,000,000", 1000000, false},
		{"Number with K Suffix", "10k", 10000, false},
		{"Number with M Suffix", "2M", 2000000, false},
		{"Number with K Suffix and Commas", "1,200k", 1200000, false},
		{"Invalid Number", "abc", 0, true},
		{"Empty String", "", 0, true},
		{"Suffix without Number", "k", 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			actual, err := parseLineCount(tc.input)
			if (err != nil) != tc.hasError {
				t.Errorf("Expected error: %v, got: %v", tc.hasError, err)
			}
			if actual != tc.expected {
				t.Errorf("Expected: %d, got: %d", tc.expected, actual)
			}
		})
	}
}

func TestResolveRange(t *testing.T) {
	testCases := []struct {
		name              string
		cfg               config
		totalLines        int64
		expectedStart     int64
		expectedEnd       int64
		hasError          bool
	}{
		{"No Range Specified", config{}, 1000, 1, 1000, false},
		{"Simple Numeric Range", config{rangeStart: "10", rangeEnd: "50"}, 1000, 10, 50, false},
		{"BOF to EOF", config{rangeStart: "BOF", rangeEnd: "EOF"}, 1000, 1, 1000, false},
		{"BOF to COF (even)", config{rangeStart: "BOF", rangeEnd: "COF"}, 1000, 1, 500, false},
		{"BOF to COF (odd)", config{rangeStart: "BOF", rangeEnd: "COF"}, 1001, 1, 501, false},
		{"COF to EOF", config{rangeStart: "COF", rangeEnd: "EOF"}, 1000, 500, 1000, false},
		{"Range Exceeds Total Lines", config{rangeStart: "100", rangeEnd: "2000"}, 1000, 100, 1000, false},
		{"Start After End", config{rangeStart: "500", rangeEnd: "100"}, 1000, 0, 0, true},
		{"Invalid Start", config{rangeStart: "abc", rangeEnd: "100"}, 1000, 0, 0, true},
		{"Invalid End", config{rangeStart: "1", rangeEnd: "xyz"}, 1000, 0, 0, true},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			start, end, err := resolveRange(tc.cfg, tc.totalLines)
			if (err != nil) != tc.hasError {
				t.Errorf("Expected error: %v, got: %v", tc.hasError, err)
			}
			if start != tc.expectedStart {
				t.Errorf("Expected start: %d, got: %d", tc.expectedStart, start)
			}
			if end != tc.expectedEnd {
				t.Errorf("Expected end: %d, got: %d", tc.expectedEnd, end)
			}
		})
	}
}

func TestGenerateName(t *testing.T) {
	cfg := config{
		baseName: "my-data",
		filename: "original.csv",
	}
	partNum := 123

	name := generateName(cfg, partNum)

	// Expected format: <baseName>_<timestamp>_<part#>.<ext>
	// Example: my-data_20231027-150405_123.csv

	// Check base name
	if !strings.HasPrefix(name, "my-data_") {
		t.Errorf("Expected name to start with 'my-data_', got: %s", name)
	}

	// Check extension
	if !strings.HasSuffix(name, ".csv") {
		t.Errorf("Expected name to end with '.csv', got: %s", name)
	}

	// Check part number
	if !strings.Contains(name, "_123.csv") {
		t.Errorf("Expected name to contain '_123.csv', got: %s", name)
	}

	// Check timestamp format with a regex
	// YYYYMMDD-HHmmss
	re := regexp.MustCompile(`\d{8}-\d{6}`)
	timestampStr := strings.TrimSuffix(strings.TrimPrefix(name, "my-data_"), "_123.csv")

	if !re.MatchString(timestampStr) {
		t.Errorf("Timestamp format is incorrect. Expected YYYYMMDD-HHmmss, got: %s", timestampStr)
	}

	// Check that the timestamp is recent (within the last 5 seconds)
	// This is a bit fragile, but it's a decent sanity check.
	timestamp, err := time.Parse("20060102-150405", timestampStr)
	if err != nil {
		t.Errorf("Could not parse timestamp from generated name: %v", err)
	}
	if time.Since(timestamp) > 5*time.Second {
		t.Errorf("Generated timestamp is not recent: %s", timestamp.String())
	}
}

// --- Integration Tests ---

// --- Test Helpers ---

func createTestDir(t *testing.T) string {
	dir, err := os.MkdirTemp("", "dts_test_*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	return dir
}

func countLinesInFile(t *testing.T, path string) int {
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("Failed to read file %s: %v", path, err)
	}
	return len(strings.Split(string(content), "\n"))
}

// --- SplitByLines Tests ---

func TestIntegrationSplitByLines_Simple(t *testing.T) {
	outputDir := createTestDir(t)
	defer os.RemoveAll(outputDir)

	cfg := config{
		filename:  "tests/test_10_lines_header.csv",
		linesN:    3,
		outputDir: outputDir,
		baseName:  "split_test",
		quiet:     true,
	}

	err := runSplit(cfg)
	if err != nil {
		t.Fatalf("runSplit failed: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(outputDir, "*"))
	if err != nil {
		t.Fatalf("Failed to glob output files: %v", err)
	}

	// 10 data lines / 3 lines per file = 3.33 -> 4 files
	if len(files) != 4 {
		t.Errorf("Expected 4 output files, got %d", len(files))
	}

	// Check line counts
	// All files should have a header + data lines
	// File 1: header + 3 lines
	// File 2: header + 3 lines
	// File 3: header + 3 lines
	// File 4: header + 1 line
	expectedLineCounts := map[string]int{
		"split_test_.*_1.csv": 4,
		"split_test_.*_2.csv": 4,
		"split_test_.*_3.csv": 4,
		"split_test_.*_4.csv": 2,
	}

	for pattern, expectedCount := range expectedLineCounts {
		var matchedFile string
		for _, file := range files {
			match, _ := regexp.MatchString(pattern, filepath.Base(file))
			if match {
				matchedFile = file
				break
			}
		}
		if matchedFile == "" {
			t.Errorf("Expected file matching pattern %s not found", pattern)
			continue
		}

		// Line count in file is tricky because of trailing newlines.
		// Let's count non-empty lines.
		content, err := os.ReadFile(matchedFile)
		if err != nil {
			t.Fatalf("Failed to read output file %s: %v", matchedFile, err)
		}
		lines := strings.Split(string(content), "\n")
		nonEmptyLines := 0
		for _, line := range lines {
			if line != "" {
				nonEmptyLines++
			}
		}

		if nonEmptyLines != expectedCount {
			t.Errorf("File %s: expected %d non-empty lines, got %d", filepath.Base(matchedFile), expectedCount, nonEmptyLines)
		}
	}
}

// --- SplitByFiles Tests ---

func TestIntegrationSplitByFiles_Simple(t *testing.T) {
	outputDir := createTestDir(t)
	defer os.RemoveAll(outputDir)

	cfg := config{
		filename:  "tests/test_10_lines_header.csv",
		filesN:    3, // Split into 3 files
		outputDir: outputDir,
		baseName:  "split_test",
		quiet:     true,
	}

	err := runSplit(cfg)
	if err != nil {
		t.Fatalf("runSplit failed: %v", err)
	}

	files, err := filepath.Glob(filepath.Join(outputDir, "*"))
	if err != nil {
		t.Fatalf("Failed to glob output files: %v", err)
	}

	if len(files) != 3 {
		t.Errorf("Expected 3 output files, got %d", len(files))
	}

	// 10 data lines / 3 files = 3 with a remainder of 1
	// Expected distribution: 4, 3, 3 data lines
	// With header: 5, 4, 4 lines
	expectedLineCounts := map[string]int{
		"split_test_.*_1.csv": 5, // 1 header + 4 data
		"split_test_.*_2.csv": 4, // 1 header + 3 data
		"split_test_.*_3.csv": 4, // 1 header + 3 data
	}

	for pattern, expectedCount := range expectedLineCounts {
		var matchedFile string
		for _, file := range files {
			match, _ := regexp.MatchString(pattern, filepath.Base(file))
			if match {
				matchedFile = file
				break
			}
		}
		if matchedFile == "" {
			t.Errorf("Expected file matching pattern %s not found", pattern)
			continue
		}

		content, err := os.ReadFile(matchedFile)
		if err != nil {
			t.Fatalf("Failed to read output file %s: %v", matchedFile, err)
		}
		lines := strings.Split(string(content), "\n")
		nonEmptyLines := 0
		for _, line := range lines {
			if line != "" {
				nonEmptyLines++
			}
		}

		if nonEmptyLines != expectedCount {
			t.Errorf("File %s: expected %d non-empty lines, got %d", filepath.Base(matchedFile), expectedCount, nonEmptyLines)
		}
	}
}
