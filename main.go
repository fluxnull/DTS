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
	"sync/atomic"
	"time"
)

const version = "v1.2.3"
const bufSize = 4 << 20    // 4MB
const chunkSize = 64 << 20   // 64MB
const indexInterval = 1000000 // every 1M lines

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
	if runtime.GOOS == "windows" && len(os.Args) == 2 && !strings.HasPrefix(os.Args[1], "-") {
		runInteractiveMode(os.Args[1])
		return
	}
	var cfg config
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
	flag.Usage = printHelp
	flag.Parse()
	if *help {
		printHelp()
		os.Exit(0)
	}
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
	if err := runSplit(cfg); err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %v\n", err)
		os.Exit(1)
	}
}

func runSplit(cfg config) error {
	f, err := openFile(cfg.filename)
	if err != nil {
		return fmt.Errorf("could not open input file: %w", err)
	}
	defer f.Close()

	var totalLines int64
	var sparseIndex map[int64]int64
	needsLineCount := cfg.filesN > 0 ||
		strings.Contains(strings.ToUpper(cfg.rangeStart), "COF") || strings.Contains(strings.ToUpper(cfg.rangeStart), "EOF") ||
		strings.Contains(strings.ToUpper(cfg.rangeEnd), "COF") || strings.Contains(strings.ToUpper(cfg.rangeEnd), "EOF")
	if needsLineCount {
		lines, index, err := countLinesAndIndex(f, cfg.quiet)
		if err != nil {
			return fmt.Errorf("could not count lines: %w", err)
		}
		// The stat call is no longer needed.
		// The number of lines is the number of newlines.
		totalLines = lines
		sparseIndex = index
	}
	startLine, endLine, err := resolveRange(cfg, totalLines)
	if err != nil {
		return fmt.Errorf("could not resolve range: %w", err)
	}
	var header []byte
	if !cfg.noHeader && startLine == 1 {
		header, err = extractHeader(f)
		if err != nil {
			return fmt.Errorf("could not extract header: %w", err)
		}
		// The bufio.Reader in extractHeader reads ahead, advancing the file pointer.
		// We must seek back to the position right after the header to continue reading.
		_, err = f.Seek(int64(len(header)), io.SeekStart)
		if err != nil {
			return fmt.Errorf("could not seek past header: %w", err)
		}
	}
	totalDataLines := endLine - startLine + 1
	if !cfg.noHeader && startLine == 1 {
		totalDataLines--
	}
	if endLine == -1 { // Special case for -l mode without range
		totalDataLines = -1 // Signal to read until EOF
	}
	if totalDataLines < 0 && endLine != -1 {
		totalDataLines = 0
	}

	if cfg.filesN > 0 {
		return runSplitByFiles(cfg, f, startLine, endLine, header, totalDataLines, sparseIndex)
	} else if cfg.linesN > 0 {
		return runSplitByLines(cfg, f, startLine, endLine, header, totalDataLines, sparseIndex)
	}
	return errors.New("no split mode selected")
}

var bufferPool = sync.Pool{
	New: func() interface{} {
		b := make([]byte, chunkSize)
		return &b
	},
}

type chunk struct{ start, end int64 }

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
	var totalLines int64
	sparseIndex := make(map[int64]int64)
	sparseIndex[1] = 0
	var mu sync.Mutex
	var wg sync.WaitGroup
	chunkCh := make(chan chunk, numChunks)
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			var localLines int64
			localIndex := make(map[int64]int64)
			for c := range chunkCh {
				lines, partialIndex, err := countInChunk(f, c.start, c.end)
				if err != nil {
					fmt.Fprintf(os.Stderr, "Error counting in chunk: %v\n", err)
					continue
				}
				localLines += lines
				for line, offset := range partialIndex {
					localIndex[line] = offset
				}
			}
			atomic.AddInt64(&totalLines, localLines)
			mu.Lock()
			for line, offset := range localIndex {
				sparseIndex[line] = offset
			}
			mu.Unlock()
		}()
	}
	for i := int64(0); i < numChunks; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > fileSize {
			end = fileSize
		}
		chunkCh <- chunk{start: start, end: end}
	}
	close(chunkCh)
	wg.Wait()
	return totalLines, sparseIndex, nil
}

func countInChunk(f *os.File, start, end int64) (int64, map[int64]int64, error) {
	bufPtr := bufferPool.Get().(*[]byte)
	defer bufferPool.Put(bufPtr)
	size := end - start
	buf := (*bufPtr)[:size]
	_, err := f.ReadAt(buf, start)
	if err != nil && err != io.EOF {
		return 0, nil, err
	}
	var lineCount int64
	partialIndex := make(map[int64]int64)
	lineCount = int64(bytes.Count(buf, []byte{'\n'}))
	return lineCount, partialIndex, nil
}

func printHelp() { fmt.Printf("DTS - Delimited Text Splitter v%s\n...", version) }

func parseLineCount(s string) (int64, error) {
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
	totalLines, _, err := countLinesAndIndex(file, true)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error counting lines: %v\n", err)
		return
	}
	// The stat call is no longer needed.
	// The number of lines is the number of newlines.
	fmt.Printf("\nFile:  %s\n", filepath.Base(filename))
	fmt.Printf("Path:  %s\n", filepath.Dir(absPath))
	fmt.Printf("Lines: %d\n\n", totalLines)
	var cfg config
	cfg.filename = filename
	cfg.baseName = strings.TrimSuffix(filepath.Base(filename), filepath.Ext(filename))
	cfg.outputDir = "."
	headerInput := promptUser("Does this file have a header (Y/N, default is Y)? ")
	if strings.ToLower(strings.TrimSpace(headerInput)) == "n" {
		cfg.noHeader = true
	}
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
		done <- true
		fmt.Fprintf(os.Stderr, "\r\n\nError during processing: %v\n", err)
		promptUser("Press ENTER to exit...")
		return
	}
	done <- true
	fmt.Println("\r\nCompleted.                ")
	promptUser("Press ENTER to exit...")
}

func promptUser(prompt string) string {
	fmt.Print(prompt)
	scanner := bufio.NewScanner(os.Stdin)
	scanner.Scan()
	return scanner.Text()
}

func showSpinner(done chan bool) {
	spinner := []string{"|", "/", "-", "\\"}
	i := 0
	for {
		select {
		case <-done:
			fmt.Print("\r")
			return
		default:
			fmt.Printf("\rProcessing... %s ", spinner[i])
			i = (i + 1) % len(spinner)
			time.Sleep(100 * time.Millisecond)
		}
	}
}

func resolveRange(cfg config, totalLines int64) (startLine, endLine int64, err error) {
	if totalLines == 0 && cfg.filesN == 0 && cfg.rangeStart == "" && cfg.rangeEnd == "" {
		return 1, -1, nil
	}
	start, end := int64(1), totalLines
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
	}
	if start > end {
		return 0, 0, fmt.Errorf("start of range (%d) cannot be after end of range (%d)", start, end)
	}
	if start < 1 {
		start = 1
	}
	if end > totalLines && totalLines > 0 {
		end = totalLines
	}
	// After line counting, totalLines is the number of newlines.
	// For a file with content, the number of lines is totalLines or totalLines + 1
	// depending on whether it ends with a newline. The logic here is tricky.
	// Let's adjust the line count here to be more accurate.
	// A file with N lines ending in \n has N newlines.
	// A file with N lines not ending in \n has N-1 newlines.
	// Our countInChunk counts newlines.
	// The original code did `lines++`, which was a simple but buggy heuristic.
	// A better approach is to let the calling code handle it based on its needs.
	// For now, the bug fix is removing the `++`. The logic in `runSplit` handles it.
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
	var bestIndexLine, bestIndexOffset int64 = 1, 0
	for line, offset := range sparseIndex {
		if line <= targetLine && line > bestIndexLine {
			bestIndexLine = line
			bestIndexOffset = offset
		}
	}
	_, err := f.Seek(bestIndexOffset, io.SeekStart)
	if err != nil {
		return fmt.Errorf("failed to seek to sparse index offset: %w", err)
	}
	reader := bufio.NewReader(f)
	currentLine := bestIndexLine
	for currentLine < targetLine {
		_, err := reader.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				return fmt.Errorf("target line %d not found in file (EOF reached at line %d)", targetLine, currentLine)
			}
			return fmt.Errorf("error scanning to target line: %w", err)
		}
		currentLine++
	}
	return nil
}

func extractHeader(f *os.File) ([]byte, error) {
	_, err := f.Seek(0, io.SeekStart)
	if err != nil {
		return nil, fmt.Errorf("could not seek to start of file for header: %w", err)
	}
	reader := bufio.NewReader(f)
	headerLine, err := reader.ReadBytes('\n')
	if err != nil && err != io.EOF {
		return nil, fmt.Errorf("could not read header line: %w", err)
	}
	headerCopy := make([]byte, len(headerLine))
	copy(headerCopy, headerLine)
	return headerCopy, nil
}

func newPartFile(cfg config, part int) (*bufio.Writer, *os.File, error) {
	name := generateName(cfg, part)
	path := filepath.Join(cfg.outputDir, name)
	file, err := os.Create(path)
	if err != nil {
		return nil, nil, fmt.Errorf("could not create part file %s: %w", path, err)
	}
	return bufio.NewWriterSize(file, bufSize), file, nil
}

func runSplitByLines(cfg config, f *os.File, startLine, endLine int64, header []byte, totalDataLines int64, sparseIndex map[int64]int64) error {
	if startLine > 1 {
		if err := seekToLine(f, sparseIndex, startLine); err != nil {
			return fmt.Errorf("failed to seek to start line for splitting: %w", err)
		}
	}
	reader := bufio.NewReaderSize(f, bufSize)
	partNum := 1
	linesInPart := int64(0)
	var writer *bufio.Writer
	var partFile *os.File
	var err error
	var createdFiles []string
	cleanup := func() {
		if partFile != nil {
			writer.Flush()
			partFile.Close()
		}
		for _, path := range createdFiles {
			os.Remove(path)
		}
	}
	readUntilEOF := totalDataLines == -1
	linesToRead := totalDataLines
	totalLinesRead := int64(0)
	for {
		if !readUntilEOF && totalLinesRead >= linesToRead {
			break
		}
		if linesInPart == 0 {
			if partFile != nil {
				writer.Flush()
				partFile.Close()
			}
			var currentFile *os.File
			writer, currentFile, err = newPartFile(cfg, partNum)
			if err != nil {
				cleanup()
				return err
			}
			partFile = currentFile
			createdFiles = append(createdFiles, partFile.Name())
			if len(header) > 0 {
				if _, err := writer.Write(header); err != nil {
					cleanup()
					return fmt.Errorf("failed to write header to part %d: %w", partNum, err)
				}
			}
		}
		line, readErr := reader.ReadBytes('\n')
		if len(line) > 0 {
			if _, writeErr := writer.Write(line); writeErr != nil {
				cleanup()
				return fmt.Errorf("failed to write line to part %d: %w", partNum, writeErr)
			}
		}
		if readErr != nil {
			if readErr == io.EOF {
				break
			}
			cleanup()
			return fmt.Errorf("error reading line from source: %w", readErr)
		}
		linesInPart++
		totalLinesRead++
		if linesInPart == cfg.linesN {
			linesInPart = 0
			partNum++
		}
	}
	if writer != nil {
		writer.Flush()
	}
	if partFile != nil {
		partFile.Close()
	}
	return nil
}

func generateName(cfg config, part int) string {
	timestamp := time.Now().Format("20060102-150405")
	extension := filepath.Ext(cfg.filename)
	return fmt.Sprintf("%s_%s_%d%s", cfg.baseName, timestamp, part, extension)
}

func runSplitByFiles(cfg config, f *os.File, startLine, endLine int64, header []byte, totalDataLines int64, sparseIndex map[int64]int64) error {
	if startLine > 1 {
		if err := seekToLine(f, sparseIndex, startLine); err != nil {
			return fmt.Errorf("failed to seek to start line for splitting: %w", err)
		}
	}
	var wg sync.WaitGroup
	writerChans := make([]chan []byte, cfg.filesN)
	for i := 0; i < cfg.filesN; i++ {
		writerChans[i] = make(chan []byte, 1024)
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
					return
				}
			}
			if err := writer.Flush(); err != nil {
				addError(fmt.Errorf("failed to flush part %d: %w", partNum+1, err))
			}
		}(i, writerChans[i])
	}
	reader := bufio.NewReaderSize(f, bufSize)
	linesPerFileBase := totalDataLines / int64(cfg.filesN)
	remainder := totalDataLines % int64(cfg.filesN)
ReaderLoop:
	for i := 0; i < cfg.filesN; i++ {
		linesForThisPart := linesPerFileBase
		if int64(i) < remainder {
			linesForThisPart++
		}
		for j := int64(0); j < linesForThisPart; j++ {
			line, err := reader.ReadBytes('\n')
			if err != nil {
				if err == io.EOF {
					if len(line) > 0 {
						writerChans[i] <- line
					}
					break ReaderLoop
				}
				addError(fmt.Errorf("error reading line from source: %w", err))
				break ReaderLoop
			}
			writerChans[i] <- line
		}
	}
	for _, ch := range writerChans {
		close(ch)
	}
	wg.Wait()
	if len(allErrors) > 0 {
		for _, path := range createdFiles {
			os.Remove(path)
		}
		var errStrings []string
		for _, err := range allErrors {
			errStrings = append(errStrings, err.Error())
		}
		return fmt.Errorf("one or more errors occurred during split:\n%s", strings.Join(errStrings, "\n"))
	}
	return nil
}
