2025-08-21 00:00:00

# Design Specification: Delimited Text Splitter (DTS) Tool

## 1. Purpose
The DTS tool is a high-performance command-line utility designed to split large delimited text files (e.g., CSV, TSV, or other tabular formats) into smaller files. It supports splitting based on a fixed number of output files or a maximum number of lines per file (excluding the header if present). The tool handles files with or without a header line, preserves original encoding and line endings exactly as in the input, and includes an optional range extraction for processing only a subset of the file's lines. All file I/O is performed in binary mode, treating content as byte streams rather than decoded text. Lines are identified solely by newline bytes ('\n', '\r\n', or '\r') without any text decoding, ensuring that whatever encoding the source file uses is preserved byte-for-byte in the outputs. This approach avoids any encoding detection, conversion, or assumptions, streaming bytes directly and only splitting at detected newline boundaries. Speed remains paramount; implemented in Go with goroutines for concurrency and parallelism in I/O, line counting (via byte scanning for newlines), and writing. The executable supports drag-and-drop for the input filename.

## 2. Scope and Assumptions
- **Input Files**: Any byte-stream file with newline-separated lines (delimited text assumed but not validated).
- **Output Files**: Byte-identical to input segments, preserving all bytes including encoding and line endings.
- **Performance**: Handle 10GB+ files; concurrent byte scanning for newlines (e.g., parallel chunk processing for '\n' counts). Target end-to-end throughput at 70-90% of hardware limits (e.g., HDD: 150-250 MB/s sequential; SATA SSD: 400-550 MB/s; NVMe: 2-7 GB/s), considering same-disk read/write caps at ~half.
- **Platform**: Cross-platform Go binary (Windows .exe, Linux, macOS), with platform-specific optimizations (e.g., FILE_FLAG_SEQUENTIAL_SCAN on Windows, posix_fadvise on Linux).
- **Encoding Handling**: Fully agnostic; binary mode only—no string decoding. Header (if present) is the byte sequence up to the first newline, copied verbatim.
- **Drag-and-Drop**: Treats dropped file as <filename>; error if no split mode without args.
- **Limitations**: Assumes newlines are consistent; no compressed inputs. Range seeks assume seekable files (e.g., local files). Advise users to place outputs on separate physical devices for max throughput if possible.

## 3. Functional Requirements
### 3.1 Core Functionality
- **Splitting Modes**:
  - By files: Equal line distribution (excluding header).
  - By lines: Up to N lines per file (excluding header).
- **Header Handling**: Default: First byte sequence to newline is header, prepended to outputs. With -nh: No header.
- **Range Extraction**: Lines from start to end (1-indexed). Acronyms:
  - BOF: 1.
  - COF: ((total_lines + 1) / 2) integer div (even: /2; odd: rounds up to higher middle, e.g., 5->3, 4->2).
  - EOF: total_lines.
  Splitting applies to extracted byte range. Header included only if start=1 and not -nh.
- **Naming**: `<output-name>_YYYYMMDD-HHmmss_<part#>.<ext>`.
- **Output**: Current dir default.
- **Quiet**: Suppress non-errors.

### 3.2 Command-Line Interface
- **Usage**:
  ```
  DTS <filename> [-f/--files N] [-l/--lines N] [-nh/--NoHeader] [-q/--quiet] [-o/--output PATH] [-n/--name STRING] [-r/--range START END]
  DTS -h/--help
  ```
- **Options**: As before, with byte-parsed N for -l.

### 3.3 Behavior Details
- **Preservation**: Binary reads/writes; detect newlines via byte scans (support '\n', '\r\n', '\r').
- **Range Calculation**:
  - Concurrent total_lines: Divide file into chunks, goroutines count newlines per chunk, sum atomically.
  - Build sparse index: During count, store offsets every K lines (e.g., K=1e6).
  - Seek: For start >1, jump to nearest index, scan bytes for remaining newlines to position reader.
  - Extract: Stream bytes from start newline to end newline.
- **Edge Cases**: Empty files, inconsistent newlines (treat as best-effort), range beyond file (truncate).
- **Error Handling**: Binary-safe errors to stderr.

### 3.4 Non-Functional Requirements
- **Performance & Concurrency**:
  - **IO Strategy**:
    - Read/write in large blocks (1-8 MB) to minimize syscalls.
    - Single pass for -lines N mode; two passes only for -files N (first for line count).
    - Advise separate physical devices for read/write to avoid halving throughput.
    - Sequential access hints: On Windows, open with FILE_FLAG_SEQUENTIAL_SCAN (via syscall or equivalent); consider FILE_FLAG_OVERLAPPED for async pipelining with manual ring buffers. On Linux, use posix_fadvise(SEQUENTIAL); optional O_DIRECT if buffers align and beneficial (measure).
    - Treat bytes as bytes: No re-encoding; just find newlines and dump.
  - **Buffering & Parsing**:
    - Scan within blocks, split on \n (handle CRLF by splitting after \n); no per-char reads.
    - Reuse buffers (e.g., sync.Pool in Go) to avoid per-line allocations.
    - Use large bufio.Writer per output file (e.g., 4MB).
    - For -files N, first pass counts \n using block reads and bytes.Count (or memchr equivalent loop).
  - **Concurrency**:
    - Pipeline: One reader goroutine → parse/split → few writers (moderate parallelism for SSD/NVMe; sequential for HDD to avoid seeks).
    - Avoid over-parallelism: Too many writers can thrash HDDs.
  - **Things to Avoid**:
    - Tiny buffers, per-line formatting/writes, excessive logging, per-line allocations, same slow HDD for outputs, antivirus on output folder.
  - **Windows-Specific**:
    - Use CreateFileW with FILE_FLAG_SEQUENTIAL_SCAN.
    - Consider ReadFile/WriteFile with OVERLAPPED + manual ring buffers.
    - MMAP via CreateFileMapping for fast scanning (measure vs std reads).
  - **Byte readers**: io.ReadSeeker for seeks; buffered for speed.
  - **Line counting/indexing**: Parallel goroutines (NumCPU), channels for results.
  - **Splitting**: Channel lines (byte slices) to concurrent writers.
  - **Memory**: Streamed; sparse index ~ file_size / (K * avg_line_bytes) entries.
- **Security**: Path sanitization.
- **Logging**: Non-quiet progress on byte processing (minimal to avoid slowdown).
- **Version**: v1.2.2 (performance optimizations).

## 4. Architecture
- **Language**: Go stdlib (os, io, bufio for byte scanning, sync, time).
- **Components**:
  - Parser: Flags with aliases.
  - Byte Counter/Indexer: Concurrent chunk scanners for newlines, build sparse offset map.
  - Range Seeker: Use index to position reader.
  - Header Extractor: Read to first newline bytes.
  - Splitter: Stream byte lines via scanner, distribute to writers.
  - Writers: Goroutines write byte slices, prepend header bytes.
- **Data Flow**:
  1. Parse.
  2. Open binary file with platform hints.
  3. Count lines/index concurrently if needed (big blocks, bytes.Count).
  4. Resolve range, seek via index/scan.
  5. Extract header bytes if applicable.
  6. Split: Scan lines as byte slices (bufio.ReadSlice or equivalent, handle partials), channel to concurrent writers (large bufio.Writer).
- **Concurrency**: Channels for byte distribution, WaitGroup sync. Tune chunk size (e.g., 64MB) for I/O balance.
- **Go Implementation Sketch**:
  ```
  // Reader: bufio.NewReaderSize(in, 4<<20)  // 4MB
  // Writer: bufio.NewWriterSize(out, 4<<20) // 4MB per output
  for {
      buf, err := r.ReadSlice('\n') // Handle ErrBufferFull by copying partial
      // Write buf to current part; rotate every N lines for -lines
  }
  // For count pass: Read big blocks, use bytes.Count(block, []byte{'\n'})
  ```

## 5. Updated Help Message
```
DTS - Delimited Text Splitter v1.2.2
===============================
High-performance splitter for large delimited files, Go-implemented with concurrency. Encoding-agnostic: treats as byte streams, preserves all bytes/line endings.
Usage:
  DTS <filename> [-f/--files N] [-l/--lines N] [-nh/--NoHeader] [-q/--quiet] [-o/--output PATH] [-n/--name STRING] [-r/--range START END]
  DTS -h/--help
Options:
  -f, --files N         Split into N equal files.
  -l, --lines N         Split into files <= N lines (excl. header). Supports K/M, commas.
  -nh, --NoHeader       No header (default assumes).
  -q, --quiet           Suppress non-errors.
  -o, --output PATH     Output dir (default current).
  -n, --name STRING     Base name (default input).
  -r, --range START END Extract lines START-END (1-indexed; BOF/COF/EOF).
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
  DTS huge.txt -f 3 -r BOF COF
  DTS export.tsv -l 300K -nh -q -o splits -n out -r 100 EOF
```

## 6. Testing Plan
- Unit: Byte newline detection (various endings), range resolver (even/odd COF), index building, buffer reuse.
- Integration: Binary files with mixed encodings, large simulated byte streams, range seeks (time benchmarks).
- Edge: No newlines, malformed bytes, deep ranges (verify index speedup), platform-specific flags.
- Performance: 10GB+ benchmarks; measure throughput vs hardware ceilings, concurrent vs sequential, single vs multi-pass.