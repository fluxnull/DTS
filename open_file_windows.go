//go:build windows

package main

import (
	"os"
	"syscall"
)

// FILE_FLAG_SEQUENTIAL_SCAN is a performance hint for the Windows file system.
const FILE_FLAG_SEQUENTIAL_SCAN = 0x08000000

// openFile on Windows uses the CreateFile syscall to provide the SEQUENTIAL_SCAN hint.
func openFile(name string) (*os.File, error) {
	// We need to convert the name to a UTF-16 pointer for the Windows syscall.
	namePtr, err := syscall.UTF16PtrFromString(name)
	if err != nil {
		return nil, err
	}

	// GENERIC_READ: Read access
	// FILE_SHARE_READ: Allow other processes to read the file
	// OPEN_EXISTING: Open the file only if it exists
	// FILE_ATTRIBUTE_NORMAL | FILE_FLAG_SEQUENTIAL_SCAN: Normal attributes + performance hint
	handle, err := syscall.CreateFile(
		namePtr,
		syscall.GENERIC_READ,
		syscall.FILE_SHARE_READ,
		nil, // Default security attributes
		syscall.OPEN_EXISTING,
		syscall.FILE_ATTRIBUTE_NORMAL|FILE_FLAG_SEQUENTIAL_SCAN,
		0, // No template file
	)

	if err != nil {
		return nil, err
	}

	// The syscall returns a handle, which we convert into a standard *os.File.
	return os.NewFile(uintptr(handle), name), nil
}
