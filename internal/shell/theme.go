package shell

import (
	"charm.land/lipgloss/v2"

	"github.com/lynxbase/lynxdb/internal/ui"
)

// ShellTheme holds SPL2 syntax highlighting styles for the shell scrollback.
type ShellTheme struct {
	Command  lipgloss.Style // cyan bold — SPL2 commands (FROM, WHERE, STATS)
	Keyword  lipgloss.Style // magenta — clause keywords (BY, AS, AND, OR)
	Function lipgloss.Style // yellow — aggregation/eval functions
	String   lipgloss.Style // green — quoted strings
	Number   lipgloss.Style // yellow — numeric literals
	Operator lipgloss.Style // white bold — comparison operators
	Pipe     lipgloss.Style // bright white bold — |
	Field    lipgloss.Style // default — identifiers/field names
}

// NewShellTheme creates a ShellTheme derived from the centralized ui colors.
func NewShellTheme() *ShellTheme {
	return &ShellTheme{
		Command:  lipgloss.NewStyle().Foreground(ui.ColorInfo()).Bold(true),
		Keyword:  lipgloss.NewStyle().Foreground(ui.ColorAccent()),
		Function: lipgloss.NewStyle().Foreground(ui.ColorWarning()),
		String:   lipgloss.NewStyle().Foreground(ui.ColorJSONStr()),
		Number:   lipgloss.NewStyle().Foreground(ui.ColorJSONNum()),
		Operator: lipgloss.NewStyle().Foreground(ui.ColorWhite()).Bold(true),
		Pipe:     lipgloss.NewStyle().Foreground(ui.ColorWhite()).Bold(true),
		Field:    lipgloss.NewStyle(),
	}
}
