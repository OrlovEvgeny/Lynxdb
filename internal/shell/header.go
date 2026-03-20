package shell

import (
	"fmt"

	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/buildinfo"
	"github.com/lynxbase/lynxdb/internal/ui"
)

// Header renders the top status line.
type Header struct {
	mode   string // "server" or "file"
	server string
	file   string
	events int
	width  int
}

// NewHeader creates a header for the given mode.
func NewHeader(mode, server, file string, events int) Header {
	return Header{
		mode:   mode,
		server: server,
		file:   file,
		events: events,
		width:  80,
	}
}

// SetWidth updates the header width.
func (h *Header) SetWidth(w int) {
	h.width = w
}

// View renders the header line.
func (h Header) View() string {
	t := ui.Stdout
	version := t.Bold.Render(fmt.Sprintf("LynxDB %s", buildinfo.Version))

	var detail string
	if h.mode == "server" {
		detail = fmt.Sprintf("connected to %s", t.Accent.Render(h.server))
	} else {
		detail = fmt.Sprintf("file mode (%s, %s events)", h.file, formatCountShell(int64(h.events)))
	}

	line := fmt.Sprintf("  %s — %s", version, detail)

	// Pad to full width.
	style := lipgloss.NewStyle().Width(h.width)

	return style.Render(line)
}

// formatCountShell is a local helper for human-readable counts.
func formatCountShell(n int64) string {
	switch {
	case n >= 1_000_000_000:
		return fmt.Sprintf("%.1fB", float64(n)/1_000_000_000)
	case n >= 1_000_000:
		return fmt.Sprintf("%.1fM", float64(n)/1_000_000)
	case n >= 10_000:
		return fmt.Sprintf("%.1fK", float64(n)/1_000)
	default:
		return fmt.Sprintf("%d", n)
	}
}
