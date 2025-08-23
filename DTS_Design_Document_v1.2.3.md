# Design Specification: Delimited Text Splitter (DTS) Tool v1.2.3

## 1. Purpose
The DTS tool is a high-performance command-line utility designed to split large delimited text files (e.g., CSV, TSV, or other tabular formats) into smaller files. It supports splitting based on a fixed number of output files or a maximum number of lines per file. For ease of use on Windows, the tool features a guided **interactive mode** when a file is dragged directly onto the executable.

The tool handles files with or without a header line, preserves original encoding and line endings exactly as in the input, and includes an optional range extraction for processing only a subset of the file's lines. All file I/O is performed in binary mode, treating content as byte streams. Speed remains paramount; implemented in Go with goroutines for concurrency and parallelism in I/O, line counting, and writing.

## 2. Scope and Assumptions
- **Input Files**: Any byte-stream file with newline-separated lines.
- **Output Files**: Byte-identical to input segments, preserving all bytes.
- **Performance**: Handle 10GB+ files; concurrent byte scanning for newlines.
- **Platform**: Cross-platform Go binary (Windows .exe, Linux, macOS), with platform-specific optimizations.
- **Encoding Handling**: Fully agnostic; binary mode only.
- **Drag-and-Drop (Windows)**: When a single file is dropped onto the executable on Windows, the tool **enters a user-friendly interactive mode** instead of requiring command-line flags. On other platforms, it treats the dropped file as the `<filename>` argument.
- **Limitations**: Assumes newlines are consistent; no compressed inputs. Range seeks assume seekable files.

## 3. Functional Requirements
### 3.1 Core Functionality
- **Splitting Modes**:
  - By files: Equal line distribution (excluding header).
  - By lines: Up to N lines per file (excluding header).
- **Header Handling**: Default: First byte sequence to newline is header, prepended to outputs. With `-nh`: No header.
- **Range Extraction**: Lines from `START` to `END` (1-indexed). `START` and `END` can each be a number or a keyword:
  - BOF: 1 (Beginning of File).
  - COF: `(total_lines + 1) / 2` (Center of File).
  - EOF: `total_lines` (End of File).
- **Naming**: `<output-name>_YYYYMMDD-HHmmss_<part#>.<ext>`.
- **Output**: Current dir default.
- **Quiet**: Suppress non-errors.

### 3.2 Command-Line Interface
- **Usage**:
  ```
  DTS <filename> [-f/--files N] [-l/--lines N] [-nh/--NoHeader] [-q/--quiet] [-o/--output PATH] [-n/--name STRING] [-r/--range START...END]
  DTS -h/--help
  ```
- **Range Flag (`-r`, `--range`)**: The range is specified with a `...` separator (e.g., `-r 1000...5000`).

### 3.3 Interactive Mode (Windows)
This mode is triggered when the executable is launched on Windows with a single argument (i.e., a file dragged onto it).
- **Workflow**:
  1.  **File Analysis**: The tool first performs a quick scan to count the total number of lines in the file.
  2.  **Display Info**: It prints a summary including the filename, path, and total line count.
  3.  **Interactive Prompts**: It guides the user through a series of questions to configure the split operation:
      - "Does this file have a header (default is Y)?"
      - "Select a split mode (1/2):" (1: By file count, 2: By line count)
      - "Enter number of files:" or "Enter max lines per file:"
  4.  **Processing**: It executes the split operation, displaying an animated spinner (`--/|\\--`) to indicate that work is in progress.
  5.  **Completion**: Upon success, it shows a "Completed." message and waits for the user to press ENTER before exiting.

## 4. Architecture
- **Language**: Go stdlib.
- **Components**: Parser, Byte Counter/Indexer, Range Seeker, Header Extractor, Splitter, Writers.
- **Data Flow**:
  1.  **Detect Mode**: On startup, check `runtime.GOOS` and `len(os.Args)`. If it matches the criteria for interactive mode on Windows, launch the interactive Q&A flow to populate the configuration.
  2.  **Parse Flags**: If not in interactive mode, parse command-line flags to populate the configuration.
  3.  **Open File**: Open the binary file with platform-specific performance hints.
  4.  **Count & Index**: Concurrently count lines and build the sparse index if required (for `-f` mode or ranges involving `COF`/`EOF`).
  5.  **Resolve & Seek**: Resolve the line range and seek to the start position.
  6.  **Extract Header**: Read header bytes if applicable.
  7.  **Split**: Stream byte slices and distribute them to the appropriate output file writers.

## 5. Updated Help Message
```
DTS - Delimited Text Splitter v1.2.3
===============================
High-performance splitter for large delimited files, Go-implemented with concurrency. Encoding-agnostic: treats as byte streams, preserves all bytes/line endings. Includes an interactive mode on Windows when a file is dragged onto the executable.

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
  BOF: 1
  COF: ((total + 1)/2) int div (odd rounds up)
  EOF: last

Behavior:
  • Byte-stream: No encoding handling, preserves exactly.
  • Header bytes prepended (if applicable).
  • Named: <name>_YYYYMMDD-HHmmss_<part#>.<ext>
  • Concurrent byte scanning/writing; optimized for large files (big blocks, pipelined).

Examples:
  DTS huge.txt -f 3 -r BOF...COF
  DTS export.tsv -l 300K -nh -q -o splits -n out -r 100...EOF
```
