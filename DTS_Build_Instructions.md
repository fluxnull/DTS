2025-08-21 00:00:00

Jules, follow these precise instructions to build the Delimited Text Splitter (DTS) tool exactly as specified in the design spec. Use Go (golang.org, version 1.21 or later) for the implementation, relying solely on the standard library (stdlib) packages like os, io, bufio, flag, sync, time, bytes, strconv, path/filepath, and errors. Do not use any external libraries or dependencies beyond stdlib to keep it lightweight and portable. The tool must be a command-line executable, cross-platform (build for Windows .exe, Linux, macOS), with performance optimizations for large files (10GB+). Focus on byte-stream handling to remain encoding-agnostic: treat everything as bytes, detect newlines ('\n', '\r\n', '\r') without decoding, and preserve all bytes exactly.

### Step 1: Project Setup
- Create a new directory: `mkdir dts && cd dts`.
- Initialize a Go module: `go mod init github.com/yourusername/dts` (replace with your actual repo if applicable).
- Create the main file: `main.go`.
- In `main.go`, start with:
  ```
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
      "strconv"
      "strings"
      "sync"
      "time"
      "runtime"
  )

  const version = "v1.2.2"
  ```
- Define constants for buffer sizes (e.g., `const bufSize = 4 << 20 // 4MB`), chunk size for parallel counting (e.g., `const chunkSize = 64 << 20 // 64MB`), and sparse index interval (e.g., `const indexInterval = 1000000 // every 1M lines`).

### Step 2: Command-Line Parsing
- Use the flag package to parse options, supporting both short and long forms. Create custom flag variables for aliases.
- Define a struct for config:
  ```
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
  ```
- In main, parse flags:
  ```
  var cfg config

  filesN := flag.Int("f", 0, "")
  flag.IntVar(filesN, "files", 0, "Split into N equal files")
  linesNStr := flag.String("l", "", "")
  flag.StringVar(linesNStr, "lines", "", "Split into files <= N lines (excl. header)")
  flag.BoolVar(&cfg.noHeader, "nh", false, "No header")
  flag.BoolVar(&cfg.noHeader, "NoHeader", false, "No header")
  flag.BoolVar(&cfg.quiet, "q", false, "Quiet mode")
  flag.BoolVar(&cfg.quiet, "quiet", false, "Quiet mode")
  flag.StringVar(&cfg.outputDir, "o", ".", "Output dir")
  flag.StringVar(&cfg.outputDir, "output", ".", "Output dir")
  flag.StringVar(&cfg.baseName, "n", "", "Base name")
  flag.StringVar(&cfg.baseName, "name", "", "Base name")
  rangeStart := flag.String("r", "", "") // Will parse later
  flag.StringVar(rangeStart, "range", "", "Extract lines START-END")

  help := flag.Bool("h", false, "Help")
  flag.BoolVar(help, "help", false, "Help")

  flag.Parse()

  if *help {
      printHelp()
      os.Exit(0)
  }

  args := flag.Args()
  if len(args) != 1 {
      fmt.Println("Error: Requires <filename>")
      printHelp()
      os.Exit(1)
  }
  cfg.filename = args[0]

  // Parse -files
  cfg.filesN = *filesN

  // Parse -lines (handle K/M, commas)
  if *linesNStr != "" {
      cfg.linesN = parseLineCount(*linesNStr)
  }

  // Parse -range (split into start/end)
  if *rangeStart != "" {
      parts := strings.SplitN(*rangeStart, " ", 2)
      if len(parts) != 2 {
          fmt.Println("Error: -range requires START END")
          os.Exit(1)
      }
      cfg.rangeStart = parts[0]
      cfg.rangeEnd = parts[1]
  }

  // Validate: exactly one split mode
  if (cfg.filesN > 0 && cfg.linesN > 0) || (cfg.filesN == 0 && cfg.linesN == 0) {
      fmt.Println("Error: Specify exactly one of -f/--files or -l/--lines")
      os.Exit(1)
  }

  // Set default baseName if empty
  if cfg.baseName == "" {
      cfg.baseName = strings.TrimSuffix(filepath.Base(cfg.filename), filepath.Ext(cfg.filename))
  }

  // Create output dir if needed
  if err := os.MkdirAll(cfg.outputDir, 0755); err != nil {
      fmt.Printf("Error creating output dir: %v\n", err)
      os.Exit(1)
  }
  ```
- Implement `printHelp()` with the exact help message from the spec.
- Implement `parseLineCount(str string) int64`: Remove commas, handle K/k (*1000), M/m (*1e6), parse to int64, error on invalid.

### Step 3: File Opening and Platform Optimizations
- Open the input file in binary mode with read-only: `f, err := os.Open(cfg.filename)`
- For platform hints:
  - On Windows: Use syscall to open with FILE_FLAG_SEQUENTIAL_SCAN. Import "syscall" and use:
    ```
    import "syscall"

    h, err := syscall.Open(cfg.filename, syscall.O_RDONLY, 0)
    if err != nil { ... }
    f := os.NewFile(uintptr(h), cfg.filename)
    // Add FILE_FLAG_SEQUENTIAL_SCAN: 0x08000000
    // You'll need to customize syscall.CreateFile for full flags, but for simplicity, note it may require wrapping.
    ```
  - On Linux: Use syscall.Fadvise(f.Fd(), 0, 0, syscall.FADV_SEQUENTIAL) after open.
  - For cross-platform, wrap in runtime.GOOS checks.
- Consider O_DIRECT on Linux if beneficial (test), but default to buffered.

### Step 4: Line Counting and Sparse Indexing (Concurrent)
- If range uses COF/EOF or split is -files, compute totalLines and build sparse index (map[int64]int64 for lineNum -> byteOffset, every indexInterval lines).
- Function `countLinesAndIndex(f *os.File, quiet bool) (int64, map[int64]int64, error)`:
  - Get file size: stat, _ := f.Stat(); size := stat.Size()
  - Num chunks: size / chunkSize + 1
  - Use runtime.NumCPU() goroutines, but cap at reasonable (e.g., 16).
  - var wg sync.WaitGroup; var mu sync.Mutex; total := int64(0); index := make(map[int64]int64)
  - Channel for chunks: ch := make(chan struct{start, end int64})
  - For each worker: go func() { for chunk := range ch { count, partialIndex := countInChunk(f, chunk.start, chunk.end); atomic.AddInt64(&total, count); mu.Lock(); merge partialIndex; mu.Unlock(); wg.Done() } }
  - Divide size into chunks, send to ch.
  - In countInChunk: Seek to start, read chunkSize bytes into buf (reuse via sync.Pool: pool.Get().([]byte)[:chunkSize]), count := bytes.Count(buf, []byte{'\n'}); adjust for overlapping newlines.
  - For index: Track cumulative lines, record offset when lineNum % indexInterval == 0.
  - Handle '\r\n' and '\r' by scanning manually if needed (loop with bytes.IndexByte for efficiency).
- If !quiet, print "Counting lines with X goroutines..."

### Step 5: Range Resolution and Seeking
- Function `resolveRange(cfg config, totalLines int64) (startLine, endLine int64)`:
  - Parse start: if "BOF" {1}, "COF" { (totalLines + 1) / 2 }, "EOF" {totalLines}, else strconv.ParseInt
  - Same for end.
  - Error if start > end or invalid.
- To seek to startLine: Use index to find nearest lower key, seek to that offset, then scan forward (bufio.Scanner or manual) counting newlines until startLine-1, position reader at start of startLine.
- For endLine: During reading, stop after (endLine - startLine + 1) lines.

### Step 6: Header Extraction
- If !cfg.noHeader && startLine == 1:
  - Create bufio.Reader(f) with bufSize.
  - header, err := r.ReadSlice('\n') // Or handle '\r'
  - Store as []byte for prepending.

### Step 7: Splitting Logic
- For -lines N (single pass preferred):
  - Stream from seek position.
  - Use bufio.ReaderSize(f, bufSize)
  - Current part = 1, lineCount = 0
  - Open new writer: func newWriter(part int) *bufio.Writer { name := generateName(cfg, part); file, _ := os.Create(filepath.Join(cfg.outputDir, name)); return bufio.NewWriterSize(file, bufSize) }
  - If header, w.Write(header)
  - Loop: buf, err := r.ReadSlice('\n') // Handle ErrBufferFull: if err == bufio.ErrBufferFull { copy partial, continue reading }
  - w.Write(buf); lineCount++
  - If lineCount == cfg.linesN { w.Flush(); close file; part++; w = newWriter(part); if header {w.Write(header)} lineCount=0 }
  - Stop if reached endLine or EOF.
- For -files N (two passes):
  - After count, compute linesPerFile = (dataLines / N), remainders.
  - Seek to start, read header if applicable.
  - Then stream, distributing to N writers concurrently: Create N writers upfront, channel lines []byte to workers.
  - Goroutine reader: read lines, send to ch := make(chan []byte, buffer)
  - N writer goroutines: receive from ch, write to their bufio.Writer, flush at end.
  - Balance: Track per-file counts, switch channel destination.
- Reuse buffers with sync.Pool: var pool = sync.Pool{New: func() any { return make([]byte, bufSize) }}
- For range: Adjust dataLines = endLine - startLine + 1 (exclude header if present).

### Step 8: Filename Generation
- Function `generateName(cfg config, part int) string`:
  - t := time.Now().Format("20060102-150405")
  - ext := filepath.Ext(cfg.filename)
  - return fmt.Sprintf("%s_%s_%d%s", cfg.baseName, t, part, ext)

### Step 9: Error Handling and Cleanup
- On errors, remove partial output files.
- Use defer for closes.
- In quiet mode, no prints except errors to stderr.

### Step 10: Building and Testing
- Build: `go build -o dts` (or dts.exe on Windows).
- Cross-build: Use GOOS=windows GOARCH=amd64 go build, etc.
- Test: Create small/large test files (e.g., 10GB with dd or generate), benchmark time go run main.go ..., compare throughput to hardware (e.g., dd if=in of=out bs=4M).
- Verify: Byte-for-byte match with originals (diff or md5), range/COF correct (e.g., odd/even totals), concurrency speedup.

Implement exactly as above; test on your machine for performance. If issues, profile with pprof.