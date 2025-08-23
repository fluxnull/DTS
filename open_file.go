//go:build !windows && !linux

package main

import "os"

// openFile is the default implementation for opening a file.
func openFile(name string) (*os.File, error) {
	return os.Open(name)
}
