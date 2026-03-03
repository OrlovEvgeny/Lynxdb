//go:build windows

package main

import "errors"

// diskFreeBytes is not implemented on Windows.
// The doctor command degrades gracefully — it simply omits the free space detail.
func diskFreeBytes(_ string) (uint64, error) {
	return 0, errors.New("disk free: not implemented on windows")
}
