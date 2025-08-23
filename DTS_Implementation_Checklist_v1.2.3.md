# Implementation Checklist

Here is a detailed checklist of all items to be implemented for the DTS tool.

**Phase 1: Setup and Configuration**
- [ ] **Project:** Create `dts` directory.
- [ ] **Project:** Initialize Go module `github.com/fluxnull/dts`.
- [ ] **Code:** Create `main.go` with initial boilerplate (imports, constants).
- [ ] **Config:** Define the `config` struct to hold all runtime parameters.

**Phase 2: User Interface**
- [ ] **CLI:** Implement parsing for all command-line flags (`-f`, `-l`, `-r`, `-nh`, `-q`, `-o`, `-n`, `-h`).
- [ ] **CLI:** Ensure `-r`/`--range` flag correctly parses the `START...END` format.
- [ ] **CLI:** Implement validation to ensure exactly one of `-f` or `-l` is used.
- [ ] **CLI:** Implement the `printHelp()` function with the full, specified help text.
- [ ] **Interactive Mode (Windows):**
    - [ ] Implement logic in `main()` to detect when to enter interactive mode.
    - [ ] Create the `runInteractiveMode()` function.
    - [ ] In interactive mode, display file stats (name, path, total lines).
    - [ ] Implement the series of interactive prompts for header, split mode, and count.
    - [ ] Implement the animated processing spinner.
    - [ ] Implement the "Press ENTER to exit" prompt on completion.

**Phase 3: Core Functionality**
- [ ] **Helpers:** Implement the `parseLineCount()` function to handle `K`/`M` suffixes.
- [ ] **File I/O:** Implement file opening with `syscall` optimizations for Windows and Linux.
- [ ] **Core Logic:** Implement the `countLinesAndIndex()` function for concurrent, chunk-based line counting and sparse index creation.
- [ ] **Core Logic:** Implement `resolveRange()` to parse keywords (`BOF`, `COF`, `EOF`) and numbers.
- [ ] **Core Logic:** Implement header extraction logic to read and store the first line as a byte slice.
- [ ] **Core Logic:** Implement the splitting algorithm for `-lines` mode (single-pass streaming).
- [ ] **Core Logic:** Implement the splitting algorithm for `-files` mode (two-pass, concurrent writers).
- [ ] **Core Logic:** Implement the `generateName()` function for creating formatted output filenames.

**Phase 4: Finalization and Verification**
- [ ] **Assembly:** Tie all components together in the `main()` function to handle all execution paths.
- [ ] **Error Handling:** Ensure robust error handling and that partial output files are deleted on failure.
- [ ] **Testing:**
    - [ ] Test `-f` mode with a sample file.
    - [ ] Test `-l` mode with a sample file.
    - [ ] Test `-r` mode with various ranges (numeric, keyword, mixed).
    - [ ] Test `-nh` and `-q` flags.
    - [ ] On Windows, test the interactive drag-and-drop mode.
    - [ ] Verify output files are byte-for-byte correct.
- [ ] **Build:** Create final cross-platform executables for Linux, Windows, and macOS.
