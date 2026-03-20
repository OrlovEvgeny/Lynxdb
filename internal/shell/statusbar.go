package shell

import (
	"fmt"
	"time"

	"charm.land/bubbles/v2/spinner"
	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
)

// Focus indicates which component has keyboard focus.
type Focus int

const (
	EditorFocus Focus = iota
	ResultsFocus
)

// StatusBar renders the bottom status line.
type StatusBar struct {
	spinner spinner.Model
	width   int
	mode    string
}

// NewStatusBar creates a status bar.
func NewStatusBar(mode string) StatusBar {
	s := spinner.New()
	s.Spinner = spinner.Dot
	s.Style = ui.Stdout.Accent

	return StatusBar{
		spinner: s,
		width:   80,
		mode:    mode,
	}
}

// SetWidth updates the status bar width.
func (sb *StatusBar) SetWidth(w int) {
	sb.width = w
}

// View renders the status bar based on current state.
func (sb StatusBar) View(focus Focus, running bool, inMulti bool, elapsed time.Duration, progress *progressMsg, tailActive bool) string {
	t := ui.Stdout
	style := lipgloss.NewStyle().
		Width(sb.width).
		Foreground(ui.ColorDim()).
		PaddingLeft(2)

	var content string

	switch {
	case tailActive:
		content = fmt.Sprintf("%s Live tail active  |  Ctrl+C: stop", sb.spinner.View())
	case running && progress != nil && progress.segmentsTotal > 0:
		el := elapsed.Round(10 * time.Millisecond)
		content = fmt.Sprintf("%s %s  %s/%s segments  %s  |  Ctrl+C: cancel",
			sb.spinner.View(),
			phaseDisplayName(progress.phase),
			formatCountShell(int64(progress.segmentsScanned)),
			formatCountShell(int64(progress.segmentsTotal)),
			t.Value.Render(el.String()))
	case running:
		el := elapsed.Round(10 * time.Millisecond)
		content = fmt.Sprintf("%s Executing... %s  |  Ctrl+C: cancel",
			sb.spinner.View(), t.Value.Render(el.String()))
	case inMulti:
		content = "Enter: continue  |  Ctrl+C: cancel                     [multi-line]"
	case focus == ResultsFocus:
		content = "Up/Down: scroll  |  Esc: editor  |  Ctrl+D: quit"
	default:
		modeTag := fmt.Sprintf("[%s]", sb.mode)
		content = fmt.Sprintf("Enter: run  |  Ctrl+D: quit  |  Tab: complete  |  /help  %s", modeTag)
	}

	return style.Render(content)
}

// phaseDisplayName maps a query execution phase to a user-friendly label.
func phaseDisplayName(phase string) string {
	switch phase {
	case "parsing":
		return "Parsing query..."
	case "scanning_buffer":
		return "Scanning buffer..."
	case "filtering_segments":
		return "Filtering segments..."
	case "scanning_segments":
		return "Scanning segments..."
	case "executing_pipeline":
		return "Executing pipeline..."
	default:
		return "Searching..."
	}
}
