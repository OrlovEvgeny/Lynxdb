package shell

import (
	"strings"
	"unicode/utf8"

	"charm.land/bubbles/v2/key"
	"charm.land/bubbles/v2/textinput"
	tea "charm.land/bubbletea/v2"
)

// Editor wraps textinput.Model with history navigation, multi-line accumulation,
// and autocomplete integration.
type Editor struct {
	input      textinput.Model
	history    *History
	completer  *Completer
	multiLine  string // accumulated buffer for lines ending with |
	prompt     string // "lynxdb> " or "lynxdb[file]> "
	contPrompt string // "    ...> "
	inMulti    bool
	keys       keyMap
}

// NewEditor creates an editor with the given prompt strings.
func NewEditor(prompt, contPrompt string, history *History, completer *Completer) Editor {
	ti := textinput.New()
	ti.Prompt = prompt
	ti.Focus()
	ti.CharLimit = 4096
	ti.ShowSuggestions = true

	return Editor{
		input:      ti,
		history:    history,
		completer:  completer,
		prompt:     prompt,
		contPrompt: contPrompt,
		keys:       defaultKeyMap(),
	}
}

// Value returns the current text input value.
func (e *Editor) Value() string {
	return e.input.Value()
}

// SetWidth updates the input width.
func (e *Editor) SetWidth(w int) {
	e.input.SetWidth(w - len(e.prompt) - 1)
}

// Update handles key events and returns commands.
func (e *Editor) Update(msg tea.Msg) (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		switch {
		case key.Matches(msg, e.keys.Submit):
			return e.handleSubmit()

		case key.Matches(msg, e.keys.Cancel):
			return e.handleCancel()

		case key.Matches(msg, e.keys.Quit):
			if e.input.Value() == "" && !e.inMulti {
				return nil, nil, &slashCommandMsg{quit: true}
			}

		case key.Matches(msg, e.keys.AcceptSugg):
			// Intercept Tab to prevent the textinput's double-fire bug:
			// its Update accepts the suggestion AND inserts \t as a space.
			if suggestion := e.input.CurrentSuggestion(); suggestion != "" {
				e.input.SetValue(suggestion)
				e.input.CursorEnd()
			}

			e.refreshSuggestions()

			return nil, nil, nil

		case key.Matches(msg, e.keys.HistPrev):
			if entry, ok := e.history.Prev(); ok {
				e.input.SetValue(entry)
				e.input.CursorEnd()
			}

			e.refreshSuggestions()

			return nil, nil, nil

		case key.Matches(msg, e.keys.HistNext):
			if entry, ok := e.history.Next(); ok {
				e.input.SetValue(entry)
				e.input.CursorEnd()
			}

			e.refreshSuggestions()

			return nil, nil, nil
		}
	}

	// Default textinput update.
	var cmd tea.Cmd
	e.input, cmd = e.input.Update(msg)

	// Update suggestions after each keystroke.
	e.refreshSuggestions()

	return cmd, nil, nil
}

// refreshSuggestions recomputes and sets suggestions, but only when
// the cursor is at the end of the input. Mid-line editing should not
// trigger ghost text or risk Tab-accepting a stale suggestion.
func (e *Editor) refreshSuggestions() {
	if e.completer == nil {
		return
	}

	value := e.input.Value()

	// Only suggest when cursor is at the end of the input.
	if e.input.Position() < utf8.RuneCountInString(value) {
		e.input.SetSuggestions(nil)
		return
	}

	suggestions := e.completer.Suggest(value)
	e.input.SetSuggestions(suggestions)
}

func (e *Editor) handleSubmit() (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	value := strings.TrimSpace(e.input.Value())
	if value == "" {
		return nil, nil, nil
	}

	// Multi-line continuation: line ends with |.
	if strings.HasSuffix(value, "|") {
		e.multiLine += value + " "
		e.inMulti = true
		e.input.SetValue("")
		e.input.Prompt = e.contPrompt

		return nil, nil, nil
	}

	fullQuery := e.multiLine + value
	e.multiLine = ""
	e.inMulti = false
	e.input.SetValue("")
	e.input.Prompt = e.prompt

	// Slash commands.
	if strings.HasPrefix(fullQuery, "/") {
		return nil, nil, &slashCommandMsg{output: fullQuery}
	}

	// Regular query.
	e.history.Add(fullQuery)
	e.history.Reset()

	return nil, &querySubmitMsg{query: fullQuery}, nil
}

func (e *Editor) handleCancel() (tea.Cmd, *querySubmitMsg, *slashCommandMsg) {
	if e.inMulti {
		e.multiLine = ""
		e.inMulti = false
		e.input.SetValue("")
		e.input.Prompt = e.prompt

		return nil, nil, nil
	}

	if e.input.Value() != "" {
		e.input.SetValue("")

		return nil, nil, nil
	}

	// Empty input + Ctrl+C → hint.
	return nil, nil, nil
}

// InMultiLine reports whether the editor is in multi-line accumulation mode.
func (e *Editor) InMultiLine() bool {
	return e.inMulti
}

// View renders the editor.
func (e *Editor) View() string {
	return e.input.View()
}
