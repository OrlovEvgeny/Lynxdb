package shell

import (
	"fmt"
	"strings"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// Completer provides context-aware SPL2 completions for the textinput.
type Completer struct {
	commands    []string
	aggFuncs    []string
	evalFuncs   []string
	slashCmds   []string
	fields      []string
	fieldValues map[string][]string // field name → top value strings
}

// NewCompleter creates a completer with static SPL2 vocabulary.
func NewCompleter() *Completer {
	return &Completer{
		commands: []string{
			"FROM", "SEARCH", "WHERE", "EVAL", "STATS", "SORT", "TABLE", "FIELDS",
			"RENAME", "HEAD", "TAIL", "DEDUP", "REX", "BIN", "TIMECHART", "TOP",
			"RARE", "STREAMSTATS", "EVENTSTATS", "JOIN", "APPEND", "MULTISEARCH",
			"TRANSACTION", "XYSERIES", "FILLNULL", "LIMIT",
		},
		aggFuncs: []string{
			"count", "sum", "avg", "min", "max", "dc", "values",
			"stdev", "perc50", "perc75", "perc90", "perc95", "perc99",
			"earliest", "latest",
		},
		evalFuncs: []string{
			"IF", "CASE", "match", "coalesce", "tonumber", "tostring",
			"round", "substr", "lower", "upper", "len", "ln",
			"mvjoin", "mvappend", "mvdedup", "isnotnull", "isnull", "strftime",
		},
		slashCmds: []string{
			"/help", "/quit", "/exit", "/clear", "/history",
			"/fields", "/sources", "/explain", "/set",
			"/format", "/server", "/timing", "/since",
			"/save", "/run", "/queries", "/tail",
		},
	}
}

// SetFields updates the dynamic field names for completion.
func (c *Completer) SetFields(fields []string) {
	c.fields = fields
}

// SetFieldValues populates known field values from the /fields API response.
func (c *Completer) SetFieldValues(infos []client.FieldInfo) {
	c.fieldValues = make(map[string][]string, len(infos))

	for _, fi := range infos {
		if len(fi.TopValues) == 0 {
			continue
		}

		vals := make([]string, 0, len(fi.TopValues))
		for _, tv := range fi.TopValues {
			if s := fmt.Sprintf("%v", tv.Value); s != "" {
				vals = append(vals, s)
			}
		}

		if len(vals) > 0 {
			c.fieldValues[fi.Name] = vals
		}
	}
}

// Suggest returns full-line completion suggestions that the textinput can match
// against the entire input value using its HasPrefix logic.
func (c *Completer) Suggest(value string) []string {
	if value == "" {
		return nil
	}

	// Slash commands — replace the entire input.
	if strings.HasPrefix(value, "/") {
		candidates := c.matchPrefix(value, c.slashCmds)
		return c.buildFullLine(value, 0, candidates, "")
	}

	// Field value pattern: field="partial → full-line with closing quote.
	if fieldName, partial, valueStart, ok := detectFieldValuePattern(value); ok {
		if values, found := c.fieldValues[fieldName]; found {
			candidates := c.matchValuePrefix(partial, values)

			// Determine whether we're inside quotes.
			hasOpenQuote := valueStart > 0 && value[valueStart-1] == '"'
			suffix := ""
			if hasOpenQuote {
				suffix = "\""
			}

			return c.buildFullLine(value, valueStart, candidates, suffix)
		}
	}

	// Find the current word being typed.
	word := lastWord(value)
	if word == "" {
		return nil
	}

	replaceStart := len(value) - len(word)

	// Context detection: what comes before the current word.
	before := strings.TrimSpace(value[:replaceStart])
	beforeLower := strings.ToLower(before)

	var candidates []string

	switch {
	// After | or at start → suggest commands.
	case before == "" || strings.HasSuffix(beforeLower, "|") || strings.HasSuffix(beforeLower, "| "):
		candidates = c.matchPrefixCI(word, c.commands)

	default:
		lastCmd := lastCommandWord(beforeLower)

		switch {
		// After stats/timechart/etc → suggest aggregation functions + fields.
		case lastCmd == "stats" || lastCmd == "timechart" || lastCmd == "eventstats" || lastCmd == "streamstats":
			candidates = c.matchPrefixCI(word, c.aggFuncs)
			candidates = append(candidates, c.matchPrefixCI(word, c.fields)...)

		// After by/where/eval → suggest field names.
		case lastCmd == "by" || lastCmd == "where" || lastCmd == "eval":
			candidates = c.matchPrefixCI(word, c.fields)

		// Default: match all candidates.
		default:
			lowerWord := strings.ToLower(word)
			for _, list := range [][]string{c.commands, c.aggFuncs, c.evalFuncs, c.fields} {
				for _, s := range list {
					if strings.HasPrefix(strings.ToLower(s), lowerWord) && strings.ToLower(s) != lowerWord {
						candidates = append(candidates, s)
					}
				}
			}
		}
	}

	return c.buildFullLine(value, replaceStart, candidates, "")
}

// suggest returns bare candidate words for the current input (internal use).
func (c *Completer) suggest(value string) []string {
	if value == "" {
		return nil
	}

	if strings.HasPrefix(value, "/") {
		return c.matchPrefix(value, c.slashCmds)
	}

	word := lastWord(value)
	if word == "" {
		return nil
	}

	lowerWord := strings.ToLower(word)
	before := strings.TrimSpace(value[:len(value)-len(word)])
	beforeLower := strings.ToLower(before)

	if before == "" || strings.HasSuffix(beforeLower, "|") || strings.HasSuffix(beforeLower, "| ") {
		return c.matchPrefixCI(word, c.commands)
	}

	lastCmd := lastCommandWord(beforeLower)
	if lastCmd == "stats" || lastCmd == "timechart" || lastCmd == "eventstats" || lastCmd == "streamstats" {
		results := c.matchPrefixCI(word, c.aggFuncs)
		results = append(results, c.matchPrefixCI(word, c.fields)...)

		return results
	}

	if lastCmd == "by" || lastCmd == "where" || lastCmd == "eval" {
		return c.matchPrefixCI(word, c.fields)
	}

	if fieldName, partial, _, ok := detectFieldValuePattern(value); ok {
		if values, found := c.fieldValues[fieldName]; found {
			return c.matchValuePrefix(partial, values)
		}
	}

	var all []string
	for _, list := range [][]string{c.commands, c.aggFuncs, c.evalFuncs, c.fields} {
		for _, s := range list {
			if strings.HasPrefix(strings.ToLower(s), lowerWord) && strings.ToLower(s) != lowerWord {
				all = append(all, s)
			}
		}
	}

	return all
}

// buildFullLine constructs full-line suggestions by replacing value[replaceStart:]
// with each candidate, appending suffix (e.g. closing quote) to each.
func (c *Completer) buildFullLine(value string, replaceStart int, candidates []string, suffix string) []string {
	if len(candidates) == 0 {
		return nil
	}

	prefix := value[:replaceStart]
	result := make([]string, 0, len(candidates))

	for _, cand := range candidates {
		full := prefix + cand + suffix
		// Skip if the suggestion exactly equals the current input.
		if full == value {
			continue
		}

		result = append(result, full)
	}

	return result
}

func (c *Completer) matchPrefix(prefix string, candidates []string) []string {
	var result []string
	lp := strings.ToLower(prefix)

	for _, s := range candidates {
		if strings.HasPrefix(strings.ToLower(s), lp) && strings.ToLower(s) != lp {
			result = append(result, s)
		}
	}

	return result
}

func (c *Completer) matchPrefixCI(word string, candidates []string) []string {
	var result []string
	lw := strings.ToLower(word)

	for _, s := range candidates {
		if strings.HasPrefix(strings.ToLower(s), lw) && strings.ToLower(s) != lw {
			result = append(result, s)
		}
	}

	return result
}

// lastWord returns the last whitespace-delimited word from s.
func lastWord(s string) string {
	s = strings.TrimRight(s, " \t")
	lastSpace := strings.LastIndexAny(s, " \t")
	if lastSpace < 0 {
		return s
	}

	return s[lastSpace+1:]
}

// detectFieldValuePattern detects field=value patterns and returns the field name,
// partial value typed so far, the byte offset where the value starts in input, and ok.
func detectFieldValuePattern(input string) (fieldName, partial string, valueStart int, ok bool) {
	trimmed := strings.TrimRight(input, " \t")
	eqIdx := strings.LastIndex(trimmed, "=")

	if eqIdx <= 0 {
		return "", "", 0, false
	}

	// Exclude !=, >=, <=
	if prev := trimmed[eqIdx-1]; prev == '!' || prev == '>' || prev == '<' {
		return "", "", 0, false
	}

	before := strings.TrimRight(trimmed[:eqIdx], " \t")
	fieldStart := strings.LastIndexAny(before, " \t|,(") + 1
	fieldName = before[fieldStart:]

	after := trimmed[eqIdx+1:]
	afterTrimmed := strings.TrimLeft(after, " \t")
	spacesSkipped := len(after) - len(afterTrimmed)

	// valueStart points into the original input, right after = plus spaces and optional quote.
	valueStart = eqIdx + 1 + spacesSkipped

	if strings.HasPrefix(afterTrimmed, "\"") {
		valueStart++ // skip the opening quote
		afterTrimmed = afterTrimmed[1:]
	}

	return fieldName, afterTrimmed, valueStart, fieldName != ""
}

func (c *Completer) matchValuePrefix(partial string, values []string) []string {
	if partial == "" {
		return values
	}

	lp := strings.ToLower(partial)

	var result []string
	for _, v := range values {
		if strings.HasPrefix(strings.ToLower(v), lp) && strings.ToLower(v) != lp {
			result = append(result, v)
		}
	}

	return result
}

// lastCommandWord returns the last significant SPL2 keyword in the text before cursor.
func lastCommandWord(s string) string {
	words := strings.Fields(s)
	for i := len(words) - 1; i >= 0; i-- {
		w := strings.TrimRight(words[i], ",|()")
		if w != "" {
			return w
		}
	}

	return ""
}
