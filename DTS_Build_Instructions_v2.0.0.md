# DTS - Detailed Build Instructions (v1.2.3)

Follow these precise instructions to build the Delimited Text Splitter (DTS) tool exactly as specified. Use Go (golang.org, version 1.21 or later) for the implementation, relying solely on the standard library.

### Introduction

The tool must be a command-line executable, cross-platform (Windows .exe, Linux, macOS), with performance optimizations for large files (10GB+). A key feature is the **Windows interactive mode**, triggered by dragging a file onto the executable, which provides a guided user experience. The implementation must remain encoding-agnostic by handling all data as byte streams.

### Step 1: Project Setup
- Create a new directory: `mkdir dts && cd dts`.
- Initialize a Go module: `go mod init github.com/fluxnull/dts`.
- Create the main file: `main.go`.
- In `main.go`, start with the specified imports and constants.

### Step 2: Application Entry Point
- In the `main` function, detect the execution mode. If run on Windows with a single file argument, call `runInteractiveMode(os.Args[1])`. Otherwise, proceed with flag parsing.

### Step 3: Command-Line Parsing
- Define the `config` struct.
- Use the `flag` package to parse all command-line options.
- Update the `-r`/`--range` flag to parse `...` as the separator.
- Implement all validation logic (e.g., ensuring `-f` or `-l` is specified).

### Step 4: Interactive Mode Implementation
- Create the `runInteractiveMode(filename string)` function.
- This function will:
  1. Analyze the file to get total lines.
  2. Display file stats.
  3. Prompt the user for configuration (header, split mode, count).
  4. Populate the `config` struct.
  5. Run the main split logic, showing an animated spinner.
  6. Wait for user confirmation to exit.

### Step 5: File Opening and Platform Optimizations
- Create an `openFile` function that uses build tags for platform-specific code.
- **Windows:** Use `syscall.CreateFile` with `FILE_FLAG_SEQUENTIAL_SCAN`.
- **Linux:** Use `os.Open` followed by `syscall.Fadvise` with `FADV_SEQUENTIAL`.
- **Other:** Use a standard `os.Open`.

### Step 6: Line Counting and Sparse Indexing (Concurrent)
- Implement the `countLinesAndIndex` function to concurrently count newlines and build a sparse index for efficient seeking. Use a worker pool and `sync.Pool` for buffers.

### Step 7: Range Resolution and Seeking
- Implement `resolveRange` to parse `BOF`, `COF`, `EOF`, and numeric boundaries.
- Implement `seekToLine` to use the sparse index to jump to the approximate location and then scan forward to the exact line.

### Step 8: Header Extraction
- Implement `extractHeader` to read the first line of the file as a byte slice, preserving the original file seek position.

### Step 9: Splitting Logic (-lines mode)
- Implement `runSplitByLines` for single-pass, streaming splitting based on a line count.

### Step 10: Splitting Logic (-files mode)
- Implement `runSplitByFiles` for two-pass, concurrent splitting into a fixed number of files. Use channels to distribute work to writer goroutines.

### Step 11: Filename Generation
- Implement `generateName` to produce filenames in the format `<baseName>_<timestamp>_<part#>.<ext>`.

### Step 12: Building and Testing
- Build the executable: `go build -o dts`.
- Cross-build using `GOOS` and `GOARCH`.
- **Test Plan:**
  - Verify CLI Mode with various flag combinations.
  - Verify Interactive Mode on Windows.
  - Benchmark performance and verify output correctness.
