package main

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/spf13/cobra"

	"github.com/lynxbase/lynxdb/internal/shell"
	"github.com/lynxbase/lynxdb/pkg/storage"
)

func init() {
	rootCmd.AddCommand(newShellCmd())
}

func newShellCmd() *cobra.Command {
	var (
		file  string
		since string
	)

	cmd := &cobra.Command{
		Use:   "shell",
		Short: "Start an interactive SPL2 shell (REPL)",
		Long: `Starts an interactive shell for running SPL2 queries.

Supports query history (arrow keys), dot commands (.help, .quit),
autocomplete (Tab), and both server and file modes.`,
		Example: `  lynxdb shell                          Connect to server
  lynxdb shell --file access.log        Query a local file
  lynxdb shell --since 1h               Default time range`,
		RunE: func(_ *cobra.Command, _ []string) error {
			if file != "" {
				return runShellFile(file, since)
			}

			return runShellServer(since)
		},
	}

	cmd.Flags().StringVarP(&file, "file", "f", "", "File mode (load file into ephemeral engine)")
	cmd.Flags().StringVarP(&since, "since", "s", "15m", "Default time range for queries")

	return cmd
}

func runShellServer(since string) error {
	return shell.Run("server", shell.RunOpts{
		Server: globalServer,
		Client: apiClient(),
		Since:  since,
	})
}

func runShellFile(file, since string) error {
	matches, err := filepath.Glob(file)
	if err != nil {
		return fmt.Errorf("invalid file pattern: %w", err)
	}
	if len(matches) == 0 {
		return fmt.Errorf("no files matching: %s", file)
	}

	eng := storage.NewEphemeralEngine()

	totalEvents := 0

	for _, path := range matches {
		f, err := os.Open(path)
		if err != nil {
			eng.Close()

			return fmt.Errorf("open %s: %w", path, err)
		}

		n, err := eng.IngestReader(f, storage.IngestOpts{Source: path})
		f.Close()

		if err != nil {
			eng.Close()

			return fmt.Errorf("ingest %s: %w", path, err)
		}

		totalEvents += n
	}

	defer eng.Close()

	return shell.Run("file", shell.RunOpts{
		Engine: eng,
		Since:  since,
		File:   file,
		Events: totalEvents,
	})
}
