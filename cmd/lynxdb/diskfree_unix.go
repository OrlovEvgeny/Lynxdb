//go:build !windows

package main

import "syscall"

// diskFreeBytes returns the number of bytes available to unprivileged users
// on the filesystem containing path.
func diskFreeBytes(path string) (uint64, error) {
	var stat syscall.Statfs_t
	if err := syscall.Statfs(path, &stat); err != nil {
		return 0, err
	}

	return stat.Bavail * uint64(stat.Bsize), nil
}
