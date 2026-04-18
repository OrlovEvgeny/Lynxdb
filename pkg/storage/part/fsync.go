package part

import (
	"fmt"
	"os"
)

// syncDir fsyncs a directory so that metadata updates such as rename are
// durable on filesystems that require the directory entry itself to be synced.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("open dir: %w", err)
	}
	defer f.Close()

	if err := f.Sync(); err != nil {
		return fmt.Errorf("sync dir: %w", err)
	}

	return nil
}
