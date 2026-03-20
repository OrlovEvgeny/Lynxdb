package shell

import "charm.land/bubbles/v2/key"

type keyMap struct {
	Submit     key.Binding
	Quit       key.Binding
	Cancel     key.Binding
	ClearScr   key.Binding
	HistPrev   key.Binding
	HistNext   key.Binding
	AcceptSugg key.Binding
	ScrollUp   key.Binding
	ScrollDn   key.Binding
	FocusBack  key.Binding
}

func defaultKeyMap() keyMap {
	return keyMap{
		Submit: key.NewBinding(
			key.WithKeys("enter"),
		),
		Quit: key.NewBinding(
			key.WithKeys("ctrl+d"),
		),
		Cancel: key.NewBinding(
			key.WithKeys("ctrl+c"),
		),
		ClearScr: key.NewBinding(
			key.WithKeys("ctrl+l"),
		),
		HistPrev: key.NewBinding(
			key.WithKeys("up"),
		),
		HistNext: key.NewBinding(
			key.WithKeys("down"),
		),
		AcceptSugg: key.NewBinding(
			key.WithKeys("tab"),
		),
		ScrollUp: key.NewBinding(
			key.WithKeys("pgup"),
		),
		ScrollDn: key.NewBinding(
			key.WithKeys("pgdown"),
		),
		FocusBack: key.NewBinding(
			key.WithKeys("esc"),
		),
	}
}
