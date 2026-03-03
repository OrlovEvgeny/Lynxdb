package main

import (
	"fmt"
	"os/exec"
	"runtime"
	"strings"
	"sync"
)

// clipboardRows holds the most recent query result for --copy.
var (
	clipMu        sync.Mutex
	clipboardRows []map[string]interface{}
)

// setClipboardRows stores rows for later clipboard copy.
func setClipboardRows(rows []map[string]interface{}) {
	clipMu.Lock()
	clipboardRows = rows
	clipMu.Unlock()
}

// writeToClipboard writes text to the system clipboard.
// Returns an error if no clipboard utility is available.
func writeToClipboard(text string) error {
	var cmd *exec.Cmd

	switch runtime.GOOS {
	case "darwin":
		cmd = exec.Command("pbcopy")
	case "windows":
		cmd = exec.Command("clip")
	default:
		// Try xclip first, fall back to xsel.
		if _, err := exec.LookPath("xclip"); err == nil {
			cmd = exec.Command("xclip", "-selection", "clipboard")
		} else if _, err := exec.LookPath("xsel"); err == nil {
			cmd = exec.Command("xsel", "--clipboard", "--input")
		} else {
			return fmt.Errorf("no clipboard utility found; install xclip or xsel")
		}
	}

	cmd.Stdin = strings.NewReader(text)

	return cmd.Run()
}

// rowsToTSV converts result rows to TSV format suitable for pasting into spreadsheets.
func rowsToTSV(rows []map[string]interface{}) string {
	if len(rows) == 0 {
		return ""
	}

	// Collect column names from first row to establish order.
	var cols []string
	for k := range rows[0] {
		cols = append(cols, k)
	}

	var b strings.Builder

	// Header row.
	b.WriteString(strings.Join(cols, "\t"))
	b.WriteByte('\n')

	// Data rows.
	for _, row := range rows {
		vals := make([]string, len(cols))
		for i, col := range cols {
			vals[i] = fmt.Sprint(row[col])
		}

		b.WriteString(strings.Join(vals, "\t"))
		b.WriteByte('\n')
	}

	return b.String()
}

// copyLastResultToClipboard copies the most recent query result to the clipboard.
// It re-reads the last result from the global clipboard buffer if set.
func copyLastResultToClipboard() error {
	clipMu.Lock()
	rows := clipboardRows
	clipMu.Unlock()

	if len(rows) == 0 {
		printWarning("No results to copy.")

		return nil
	}

	tsv := rowsToTSV(rows)
	if err := writeToClipboard(tsv); err != nil {
		return fmt.Errorf("clipboard: %w", err)
	}

	printSuccess("Copied %d rows to clipboard (TSV format)", len(rows))

	return nil
}
