//go:build linux

package main

import (
	"os"
	"syscall"
)

// openFile on Linux uses Fadvise to hint sequential read access.
func openFile(name string) (*os.File, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, err
	}

	// Use Fadvise to tell the kernel that we are doing sequential reads.
	// This can improve performance by optimizing file caching.
	err = syscall.Fadvise(int(file.Fd()), 0, 0, syscall.FADV_SEQUENTIAL)
	if err != nil {
		// Fadvise failure is not critical, we can still proceed.
		// We can log this error if not in quiet mode.
		// For now, we'll just ignore the error and return the file.
	}

	return file, nil
}
