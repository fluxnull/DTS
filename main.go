package main

import (
	"bufio"
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"time"
)

const version = "v1.2.3"
const bufSize = 4 << 20    // 4MB
const chunkSize = 64 << 20   // 64MB
const indexInterval = 1000000 // every 1M lines

// config holds all the configuration for the DTS tool.
type config struct {
	filename   string
	filesN     int
	linesN     int64
	noHeader   bool
	quiet      bool
	outputDir  string
	baseName   string
	rangeStart string
	rangeEnd   string
}

func main() {
	// --- Interactive Mode Detection ---
	// If on Windows and run with a single argument (the file to be split),
	// enter interactive mode.
	if runtime.GOOS == "windows" && len(os.Args) == 2 && !strings.HasPrefix(os.Args[1], "-") {
		runInteractiveMode(os.Args[1])
		return
	}

	var cfg config

	// Flag definitions
	filesN := flag.Int("f", 0, "Split into N equal files")
	flag.IntVar(filesN, "files", 0, "Alias for -f")

	linesNStr := flag.String("l", "", "Split into files <= N lines (excl. header)")
	flag.StringVar(linesNStr, "lines", "", "Alias for -l")

	flag.BoolVar(&cfg.noHeader, "nh", false, "No header")
	flag.BoolVar(&cfg.noHeader, "NoHeader", false, "Alias for -nh")

	flag.BoolVar(&cfg.quiet, "q", false, "Quiet mode")
	flag.BoolVar(&cfg.quiet, "quiet", false, "Alias for -q")

	flag.StringVar(&cfg.outputDir, "o", ".", "Output dir")
	flag.StringVar(&cfg.outputDir, "output", ".", "Alias for -o")

	flag.StringVar(&cfg.baseName, "n", "", "Base name for output files")
	flag.StringVar(&cfg.baseName, "name", "", "Alias for -n")

	rangeStr := flag.String("r", "", "Extract lines START...END")
	flag.StringVar(rangeStr, "range", "", "Alias for -r")

	help := flag.Bool("h", false, "Show help message")
	flag.BoolVar(help, "help", false, "Alias for -h")

	flag.Usage = printHelp // Set custom usage function
	flag.Parse()

	if *help {
		printHelp()
		os.Exit(0)
	}

	// --- Arguments and Validation ---

	args := flag.Args()
	if len(args) != 1 {
		fmt.Fprintln(os.Stderr, "Error: A single <filename> argument is required.")
		printHelp()
		os.Exit(1)
	}
	cfg.filename = args[0]

	cfg.filesN = *filesN

	isLinesSet := *linesNStr != ""
	if isLinesSet {
		linesN, err := parseLineCount(*linesNStr)
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error: Invalid value for -l/--lines: %v\n", err)
			os.Exit(1)
		}
		cfg.linesN = linesN
	}

	if *rangeStr != "" {
		parts := strings.SplitN(*rangeStr, "...", 2)
		if len(parts) != 2 {
			fmt.Fprintln(os.Stderr, "Error: -range requires START...END format")
			os.Exit(1)
		}
		cfg.rangeStart = parts[0]
		cfg.rangeEnd = parts[1]
	}

	isFilesSet := cfg.filesN > 0
	if (isFilesSet && isLinesSet) || (!isFilesSet && !isLinesSet) {
		fmt.Fprintln(os.Stderr, "Error: Specify exactly one of -f/--files or -l/--lines")
		os.Exit(1)
	}

	if cfg.baseName == "" {
		cfg.baseName = strings.TrimSuffix(filepath.Base(cfg.filename), filepath.Ext(cfg.filename))
	}

	if err := os.MkdirAll(cfg.outputDir, 0755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating output dir: %v\n", err)
		os.Exit(1)
	}

	// This is where the main logic will go.
	if err := runSplit(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
		os.Exit(1)
	}
}

// runSplit is the main orchestration function.
func runSplit(cfg config) error {
	f, err := openFile(cfg.filename)
	if err != nil {
		return fmt.Errorf("could not open input file: %w", err)
	}
	defer f.Close()

	var totalLines int64
	var sparseIndex map[int64]int64

	// Line counting is needed for -f mode or if range involves keywords.
	needsLineCount := cfg.filesN > 0 ||
		strings.Contains(strings.ToUpper(cfg.rangeStart), "COF") || strings.Contains(strings.ToUpper(cfg.rangeStart), "EOF") ||
		strings.Contains(strings.ToUpper(cfg.rangeEnd), "COF") || strings.Contains(strings.ToUpper(cfg.rangeEnd), "EOF")

	if needsLineCount {
		lines, index, err := countLinesAndIndex(f, cfg.quiet)
		if err != nil {
			return fmt.Errorf("could not count lines: %w", err)
		}
		// The line count is the number of newlines. If the file doesn't end
		// with a newline, the last line won't be counted, which is a slight inaccuracy
		// this tool accepts for performance. The `+1` logic was buggy.
		totalLines = lines
		sparseIndex = index
	}

	startLine, endLine, err := resolveRange(cfg, totalLines)
	if err != nil {
		return fmt.Errorf("could not resolve range: %w", err)
	}

	// Create a single buffered reader that will be used for all subsequent reads.
	// This avoids "competing reader" bugs.
	// We must seek to the start of the file first before creating the reader.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("could not seek to start of file: %w", err)
	}
	reader := bufio.NewReaderSize(f, bufSize)
	
	var header []byte
	if !cfg.noHeader && startLine == 1 {
		header, err = extractHeader(reader)
		if err != nil {
			return fmt.Errorf("could not extract header: %w", err)
		}
	}

	// The actual number of lines to be processed
	totalDataLines := endLine - startLine + 1
	if !cfg.noHeader && startLine == 1 {
		totalDataLines--
	}
	if totalDataLines < 0 {
		totalDataLines = 0
	}


	if !cfg.quiet {
		fmt.Printf("Processing %d lines from line %d to %d...\n", totalDataLines, startLine, endLine)
	}


	if cfg.filesN > 0 {
		return runSplitByFiles(cfg, f, reader, startLine, endLine, header, totalDataLines, sparseIndex)
	} else if cfg.linesN > 0 {
		return runSplitByLines(cfg, f, reader, startLine, endLine, header, sparseIndex)
	}

	return errors.New("no split mode selected (this should not be reached)")
}


// --- Core Logic ---

var bufferPool = sync.Pool{
	New: func() interface{} {
		// The buffers need to be at least chunkSize.
		// We'll rely on the caller to slice it to the correct size.
		b := make([]byte, chunkSize)
		return &b
	},
}

type chunk struct {
	start int64
	end   int64
	index int64
}

type chunkResult struct {
	chunkIndex   int64
	lineCount    int64
	partialIndex map[int64]int64
}

// countLinesAndIndex counts the number of lines and builds a sparse index of line number to byte offset.
// It uses a parallel map-reduce strategy: workers process chunks in parallel, and the main
// goroutine reduces their results sequentially to build the final index.
func countLinesAndIndex(f *os.File, quiet bool) (int64, map[int64]int64, error) {
	stat, err := f.Stat()
	if err != nil {
		return 0, nil, err
	}
	fileSize := stat.Size()

	if fileSize == 0 {
		return 0, make(map[int64]int64), nil
	}

	numChunks := fileSize / chunkSize
	if fileSize%chunkSize != 0 {
		numChunks++
	}
	numWorkers := runtime.NumCPU()
	if numWorkers > 16 {
		numWorkers = 16
	}
	if numWorkers > int(numChunks) {
		numWorkers = int(numChunks)
	}
	if !quiet {
		fmt.Printf("Counting lines with %d goroutines...\n", numWorkers)
	}

	var wg sync.WaitGroup
	chunkCh := make(chan chunk, numChunks)
	resultsCh := make(chan chunkResult, numChunks)
	errCh := make(chan error, numWorkers)

	// Start worker pool (Map phase)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for c := range chunkCh {
				lines, pIndex, err := countInChunk(f, c.start, c.end)
				if err != nil {
					errCh <- fmt.Errorf("error in chunk %d: %w", c.index, err)
					return
				}
				resultsCh <- chunkResult{chunkIndex: c.index, lineCount: lines, partialIndex: pIndex}
			}
		}()
	}

	// Distribute work
	for i := int64(0); i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > fileSize {
			end = fileSize
		}
		chunkCh <- chunk{start: start, end: end, index: i}
	}
	close(chunkCh)

	wg.Wait()
	close(resultsCh)
	close(errCh)

	// Check for errors from workers
	if len(errCh) > 0 {
		return 0, nil, <-errCh // Return the first error found
	}

	// --- Reduce phase (sequential) ---
	results := make([]chunkResult, numChunks)
	for res := range resultsCh {
		results[res.chunkIndex] = res // Store results in order using the chunk index
	}

	var totalNewlines int64
	sparseIndex := make(map[int64]int64)
	sparseIndex[1] = 0 // Line 1 always starts at offset 0

	var newlinesBeforeThisChunk int64 = 0
	for _, res := range results {
		for localNewlineCount, byteOffset := range res.partialIndex {
			// The global line number is the count of all newlines in previous chunks,
			// plus the count of newlines in this chunk so far, plus one (for 1-based indexing).
			globalLineNum := newlinesBeforeThisChunk + localNewlineCount + 1
			sparseIndex[globalLineNum] = byteOffset
		}
		totalNewlines += res.lineCount
		newlinesBeforeThisChunk += res.lineCount
	}

	// The caller expects the number of newlines, and will add 1 if the file is not empty.
	return totalNewlines, sparseIndex, nil
}

// countInChunk reads a chunk of a file, counts the newlines within it, and builds a
// sparse index of local line numbers to global byte offsets.
func countInChunk(f *os.File, start, end int64) (int64, map[int64]int64, error) {
	size := end - start
	if size <= 0 {
		return 0, nil, nil
	}

	bufPtr := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(bufPtr)
	buf := *bufPtr

	// Ensure buffer is large enough.
	if int64(len(buf)) < size {
		buf = make([]byte, size)
	} else {
		buf = buf[:size]
	}

	n, err := f.ReadAt(buf, start)
	if err != nil && err != io.EOF {
		return 0, nil, err
	}
	// Process only the bytes that were actually read.
	buf = buf[:n]

	var newlineCount int64
	partialIndex := make(map[int64]int64)
	currentOffsetInBuf := 0

	for {
		idx := bytes.IndexByte(buf[currentOffsetInBuf:], '\n')
		if idx == -1 {
			break // No more newlines in this part of the buffer
		}

		newlineCount++

		// Check if this newline corresponds to a line number we need to index.
		// The key for the partial index is the count of newlines *within this chunk*.
		if newlineCount%indexInterval == 0 {
			// The value is the global byte offset of the character *after* the newline,
			// which is the start of the next line.
			offsetInFile := start + int64(currentOffsetInBuf) + int64(idx) + 1
			partialIndex[newlineCount] = offsetInFile
		}

		currentOffsetInBuf += idx + 1
	}

	return newlineCount, partialIndex, nil
}

// printHelp displays the detailed help message for the tool.
func printHelp() {
	fmt.Printf(`DTS - Delimited Text Splitter v%s
===============================
High-performance splitter for large delimited files, Go-implemented with concurrency.
Encoding-agnostic: treats as byte streams, preserves all bytes/line endings.
Includes an interactive mode on Windows when a file is dragged onto the executable.

Usage:
  DTS <filename> [-f/--files N] [-l/--lines N] [-nh/--NoHeader] [-q/--quiet] [-o/--output PATH] [-n/--name STRING] [-r/--range START...END]
  DTS -h/--help

Options:
  -f, --files N         Split into N equal files.
  -l, --lines N         Split into files <= N lines (excl. header). Supports K/M, commas.
  -nh, --NoHeader       No header (default assumes).
  -q, --quiet           Suppress non-errors.
  -o, --output PATH     Output dir (default current).
  -n, --name STRING     Base name (default input).
  -r, --range START...END Extract lines START...END (1-indexed).
  -h, --help            Help.

Range:
  START and END can be a number or a keyword.
  BOF: 1 (Beginning of File)
  COF: ((total + 1)/2) (Center of File)
  EOF: last line (End of File)

Behavior:
  • Byte-stream: No encoding handling, preserves exactly.
  • Header bytes prepended (if applicable).
  • Named: <name>_YYYYMMDD-HHmmss_<part#>.<ext>
  • Concurrent byte scanning/writing; optimized for large files (big blocks, pipelined).

Examples:
  DTS huge.txt -f 3 -r BOF...COF
  DTS export.tsv -l 300K -nh -q -o splits -n out -r 100...EOF
`, version)
}

// parseLineCount converts a string with optional K/M suffixes and commas into an int64.
func parseLineCount(s string) (int64, error) {
	// Remove commas for numbers like 1,000,000
	cleanStr := strings.ReplaceAll(s, ",", "")
	cleanStr = strings.ToLower(cleanStr)

	var multiplier int64 = 1
	if strings.HasSuffix(cleanStr, "k") {
		multiplier = 1000
		cleanStr = strings.TrimSuffix(cleanStr, "k")
	} else if strings.HasSuffix(cleanStr, "m") {
		multiplier = 1_000_000
		cleanStr = strings.TrimSuffix(cleanStr, "m")
	}

	val, err := strconv.ParseInt(strings.TrimSpace(cleanStr), 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid number format: %w", err)
	}

	return val * multiplier, nil
}

// --- Interactive Mode ---

func runInteractiveMode(filename string) {
	fmt.Printf("DTS - Delimited Text Splitter v%s\n\n", version)
	fmt.Println("Running in Interactive Mode...")

	absPath, err := filepath.Abs(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error getting absolute path: %v\n", err)
		return
	}

	file, err := openFile(filename)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error opening file: %v\n", err)
		return
	}
	defer file.Close()

	fmt.Println("Analyzing file, please wait...")
	totalLines, _, err := countLinesAndIndex(file, true) // Run in quiet mode for interactive
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error counting lines: %v\n", err)
		return
	}
	// The line count is the number of newlines. The buggy `+1` logic is removed.


	fmt.Printf("\nFile:  %s\n", filepath.Base(filename))
	fmt.Printf("Path:  %s\n", filepath.Dir(absPath))
	fmt.Printf("Lines: %d\n\n", totalLines)

	var cfg config
	cfg.filename = filename
	cfg.baseName = strings.TrimSuffix(filepath.Base(filename), filepath.Ext(cfg.filename))
	cfg.outputDir = "." // Default output dir

	// Prompt for header
	headerInput := promptUser("Does this file have a header (Y/N, default is Y)? ")
	if strings.ToLower(strings.TrimSpace(headerInput)) == "n" {
		cfg.noHeader = true
	}

	// Prompt for split mode
	for {
		modeInput := promptUser("Select a split mode (1: By file count, 2: By line count): ")
		if strings.TrimSpace(modeInput) == "1" {
			countStr := promptUser("Enter number of files: ")
			count, err := strconv.Atoi(strings.TrimSpace(countStr))
			if err == nil && count > 0 {
				cfg.filesN = count
				break
			}
			fmt.Println("Invalid number. Please try again.")
		} else if strings.TrimSpace(modeInput) == "2" {
			countStr := promptUser("Enter max lines per file: ")
			count, err := parseLineCount(countStr)
			if err == nil && count > 0 {
				cfg.linesN = count
				break
			}
			fmt.Println("Invalid number. Please try again.")
		} else {
			fmt.Println("Invalid selection. Please enter 1 or 2.")
		}
	}

	fmt.Println("\nConfiguration complete. Starting process...")
	
	done := make(chan bool)
	go showSpinner(done)

	if err := runSplit(cfg); err != nil {
		// Stop the spinner and print the error
		done <- true
		fmt.Fprintf(os.Stderr, "\r\n\nError during processing: %v\n", err)
		promptUser("Press ENTER to exit...")
		return
	}

	done <- true
	fmt.Println("\r\nCompleted.                ") // Extra spaces to clear spinner line
	promptUser("Press ENTER to exit...")
}

// promptUser displays a prompt and returns the user's input.
func promptUser(prompt string) string {
	fmt.Print(prompt)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	return scanner.Text()
}

// showSpinner displays an animated spinner until a signal is received on the done channel.
func showSpinner(done chan bool) {
	spinner := []string{"|", "/", "-", "\\"}
	i := 0
	for {
		select {
		case <-done:
			fmt.Print("\r") // Clear the spinner line
			return
		default:
			fmt.Printf("\rProcessing... %s ", spinner[i])
			i = (i + 1) % len(spinner)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

// --- Range and Seek Logic ---

func resolveRange(cfg config, totalLines int64) (startLine, endLine int64, err error) {
	// Default to full range if not specified.
	// endLine = -1 will signify processing until EOF.
	start, end := int64(1), int64(-1)
	if totalLines > 0 {
		end = totalLines // If we have a line count, use it as the default end.
	}

	if cfg.rangeStart != "" {
		start, err = parseRangeBoundary(cfg.rangeStart, totalLines)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid start of range: %w", err)
		}
	}

	if cfg.rangeEnd != "" {
		end, err = parseRangeBoundary(cfg.rangeEnd, totalLines)
		if err != nil {
			return 0, 0, fmt.Errorf("invalid end of range: %w", err)
		}
	} else if totalLines > 0 {
		// If no end range was specified, but we have a total line count, use it.
		end = totalLines
	}

	// Only validate the start vs end if the end is not "until EOF".
	if end != -1 && start > end {
		return 0, 0, fmt.Errorf("start of range (%d) cannot be after end of range (%d)", start, end)
	}

	if start < 1 {
		start = 1
	}
	if totalLines > 0 && end > totalLines {
		end = totalLines
	}

	return start, end, nil
}

func parseRangeBoundary(s string, totalLines int64) (int64, error) {
	s = strings.ToUpper(strings.TrimSpace(s))
	switch s {
	case "BOF":
		return 1, nil
	case "COF":
		if totalLines == 0 {
			return 1, nil
		}
		return (totalLines + 1) / 2, nil
	case "EOF":
		if totalLines == 0 {
			return 1, nil
		}
		return totalLines, nil
	default:
		return strconv.ParseInt(s, 10, 64)
	}
}

func seekToLine(f *os.File, sparseIndex map[int64]int64, targetLine int64) error {
	if targetLine <= 1 {
		_, err := f.Seek(0, io.SeekStart)
		return err
	}

	// Find the best offset in the sparse index to start from
	var bestIndexLine, bestIndexOffset int64 = 1, 0
	for line, offset := range sparseIndex {
		if line <= targetLine && line > bestIndexLine {
			bestIndexLine = line
			bestIndexOffset = offset
		}
	}

	// Seek to the nearest known offset
	_, err := f.Seek(bestIndexOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to sparse index offset: %w", err)
	}

	// Scan forward from that point using an unbuffered read to avoid the
	// read-ahead issue that bufio.Reader can cause.
	currentLine := bestIndexLine
	if currentLine >= targetLine {
		return nil
	}

	b := make([]byte, 1)
	for {
		_, err := f.Read(b)
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("target line %d not found in file (EOF reached at line %d)", targetLine, currentLine)
			}
			return fmt.Errorf("error scanning to target line: %w", err)
		}
		if b[0] == '\n' {
			currentLine++
			if currentLine >= targetLine {
				break
			}
		}
	}

	return nil
}

// --- Header Logic ---

// extractHeader reads the first line from the provided buffered reader.
func extractHeader(reader *bufio.Reader) ([]byte, error) {
	headerLine, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("could not read header line: %w", err)
	}

	// The returned slice from ReadBytes is only valid until the next read.
	// We must make a copy to preserve it.
	headerCopy := make([]byte, len(headerLine))
	copy(headerCopy, headerLine)

	return headerCopy, nil
}

// --- Splitting Logic ---

// newPartFile creates a new output file for a part, returning a buffered writer and the file handle.
func newPartFile(cfg config, part int) (*bufio.Writer, *os.File, error) {
	name := generateName(cfg, part)
	path := filepath.Join(cfg.outputDir, name)
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create part file %s: %w", path, err)
	}
	return bufio.NewWriterSize(file, bufSize), file, nil
}

// runSplitByLines handles the splitting logic for the -l/--lines mode.
// It supports true streaming when no end range is specified (endLine == -1).
func runSplitByLines(cfg config, f *os.File, reader *bufio.Reader, startLine, endLine int64, header []byte, sparseIndex map[int64]int64) error {
	// If startLine > 1, we need to seek to it. If startLine is 1, the file pointer
	// is already correctly positioned.
	if startLine > 1 {
		if err := seekToLine(f, sparseIndex, startLine); err != nil {
			return fmt.Errorf("failed to seek to start line for splitting: %w", err)
		}
		// After seeking the underlying file, we MUST reset the buffered reader
		// to discard its old buffer, which is now invalid.
		reader.Reset(f)
	}

	partNum := 1
	linesInPart := int64(0)
	var totalLinesRead int64

	var writer *bufio.Writer
	var partFile *os.File
	var createdFiles []string

	cleanup := func() {
		if partFile != nil {
			if writer != nil {
				writer.Flush()
			}
			partFile.Close()
		}
		for _, path := range createdFiles {
			os.Remove(path)
		}
	}

	// Loop until we explicitly break on EOF or error.
	for {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break // Normal exit condition: we've read the whole file.
			}
			cleanup()
			return fmt.Errorf("error reading line from source: %w", err)
		}

		// If we have a line, we may need to create a new part file for it.
		if linesInPart == 0 {
			// Close the previous part file if it exists.
			if partFile != nil {
				writer.Flush()
				partFile.Close()
			}
			// Create the new part file.
			var currentFile *os.File
			writer, currentFile, err = newPartFile(cfg, partNum)
			if err != nil {
				cleanup()
				return err
			}
			partFile = currentFile
			createdFiles = append(createdFiles, partFile.Name())

			// Write the header to the new file.
			if len(header) > 0 {
				if _, err := writer.Write(header); err != nil {
					cleanup()
					return fmt.Errorf("failed to write header to part %d: %w", partNum, err)
				}
			}
		}

		// Write the line we just read.
		if _, writeErr := writer.Write(line); writeErr != nil {
			cleanup()
			return fmt.Errorf("failed to write line to part %d: %w", partNum, writeErr)
		}

		linesInPart++
		totalLinesRead++

		// If a specific range is being processed, check if we're done.
		linesToRead := endLine - startLine + 1
		if !cfg.noHeader && startLine == 1 {
			linesToRead--
		}
		if endLine != -1 && totalLinesRead >= linesToRead {
			break
		}

		// If the current part file is full, roll over to the next part number.
		if linesInPart == cfg.linesN {
			linesInPart = 0
			partNum++
		}
	}

	// Final flush and close for the last written file.
	if writer != nil {
		writer.Flush()
	}
	if partFile != nil {
		partFile.Close()
	}

	return nil
}

// generateName creates a new filename for a split part based on the configuration.
// The format is <baseName>_<timestamp>_<part#>.<ext>.
func generateName(cfg config, part int) string {
	timestamp := time.Now().Format("20060102-150405")
	extension := filepath.Ext(cfg.filename)
	// Example: mydata_20231027-083000_1.csv
	return fmt.Sprintf("%s_%s_%d%s", cfg.baseName, timestamp, part, extension)
}

// runSplitByFiles handles the splitting logic for the -f/--files mode.
func runSplitByFiles(cfg config, f *os.File, reader *bufio.Reader, startLine, endLine int64, header []byte, totalDataLines int64, sparseIndex map[int64]int64) error {
	if totalDataLines == 0 {
		return nil // Nothing to split
	}

	// If startLine > 1, we need to seek. After seeking, the reader must be reset.
	if startLine > 1 {
		if err := seekToLine(f, sparseIndex, startLine); err != nil {
			return fmt.Errorf("failed to seek to start line for splitting: %w", err)
		}
		reader.Reset(f)
	}

	// --- Worker setup ---
	var wg sync.WaitGroup
	writerChans := make([]chan []byte, cfg.filesN)
	for i := 0; i < cfg.filesN; i++ {
		writerChans[i] = make(chan []byte, 1024) // Buffered channel
	}

	var allErrors []error
	var errMu sync.Mutex
	addError := func(err error) {
		errMu.Lock()
		allErrors = append(allErrors, err)
		errMu.Unlock()
	}

	var createdFiles []string
	var fileMu sync.Mutex

	for i := 0; i < cfg.filesN; i++ {
		wg.Add(1)
		go func(partNum int, ch <-chan []byte) {
			defer wg.Done()

			// Don't create a file if there are no lines for it.
			linesPerFile := totalDataLines / int64(cfg.filesN)
			remainder := totalDataLines % int64(cfg.filesN)
			if linesPerFile == 0 && int64(partNum) >= remainder {
				return // This part gets 0 lines, so do nothing.
			}

			writer, partFile, err := newPartFile(cfg, partNum+1)
			if err != nil {
				addError(err)
				return
			}
			defer partFile.Close()

			fileMu.Lock()
			createdFiles = append(createdFiles, partFile.Name())
			fileMu.Unlock()

			if len(header) > 0 {
				if _, err := writer.Write(header); err != nil {
					addError(fmt.Errorf("failed to write header to part %d: %w", partNum+1, err))
					return
				}
			}

			for line := range ch {
				if _, err := writer.Write(line); err != nil {
					addError(fmt.Errorf("failed to write line to part %d: %w", partNum+1, err))
					return // Stop processing for this writer on error
				}
			}

			if err := writer.Flush(); err != nil {
				addError(fmt.Errorf("failed to flush part %d: %w", partNum+1, err))
			}
		}(i, writerChans[i])
	}

	// --- Main reader logic (Corrected) ---
	linesPerFile := totalDataLines / int64(cfg.filesN)
	remainder := totalDataLines % int64(cfg.filesN)

	var partIndex int
	var linesReadForCurrentPart int64
	var totalLinesRead int64

	for totalLinesRead < totalDataLines {
		line, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break // End of file is expected
			}
			addError(fmt.Errorf("error reading line from source: %w", err))
			break // Stop on read error
		}

		// Make a copy of the line, as the buffer will be reused.
		lineCopy := make([]byte, len(line))
		copy(lineCopy, line)

		// Determine the number of lines this part should have.
		numLinesForThisPart := linesPerFile
		if int64(partIndex) < remainder {
			numLinesForThisPart++
		}

		// If the part is supposed to get lines, send it.
		if numLinesForThisPart > 0 {
			writerChans[partIndex] <- lineCopy
			linesReadForCurrentPart++
		}

		totalLinesRead++

		// If the current part is full, move to the next part.
		if linesReadForCurrentPart >= numLinesForThisPart && partIndex < cfg.filesN-1 {
			linesReadForCurrentPart = 0
			partIndex++
		}
	}

	// Close channels and wait for writers
	for _, ch := range writerChans {
		close(ch)
	}
	wg.Wait()

	if len(allErrors) > 0 {
		// Cleanup all created files on error
		for _, path := range createdFiles {
			os.Remove(path)
		}
		// Return a combined error
		var errStrings []string
		for _, err := range allErrors {
			errStrings = append(errStrings, err.Error())
		}
		return fmt.Errorf("one or more errors occurred during split:\n%s", strings.Join(errStrings, "\n"))
	}

	return nil
}
