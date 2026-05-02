package syslog

import (
	"fmt"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/lynxbase/lynxdb/pkg/config"
	"github.com/lynxbase/lynxdb/pkg/event"
)

const (
	dialectRFC5424 = "rfc5424"
	dialectRFC3164 = "rfc3164"
	dialectRaw     = "raw"
)

var facilityLabels = []string{
	"kern", "user", "mail", "daemon", "auth", "syslog", "lpr", "news",
	"uucp", "cron", "authpriv", "ftp", "ntp", "security", "console", "solaris-cron",
	"local0", "local1", "local2", "local3", "local4", "local5", "local6", "local7",
}

var severityLabels = []string{
	"emerg", "alert", "crit", "err", "warning", "notice", "info", "debug",
}

type parser struct {
	cfg *runtimeConfig
}

func newParser(cfg *runtimeConfig) *parser {
	return &parser{cfg: cfg}
}

func (p *parser) parse(raw []byte, source string, receivedAt time.Time) (*event.Event, string) {
	line := string(raw)
	dialect := strings.ToLower(p.cfg.Parser)
	if dialect == "" || dialect == "auto" {
		dialect = detectDialect(line)
	}

	switch dialect {
	case dialectRFC5424:
		e, err := p.parseRFC5424(line, source, receivedAt)
		if err == nil {
			return e, dialectRFC5424
		}
		e = p.rawEvent(line, source, receivedAt)
		e.ParseError = true
		return e, dialectRaw
	case dialectRFC3164:
		e, err := p.parseRFC3164(line, source, receivedAt)
		if err == nil {
			return e, dialectRFC3164
		}
		e = p.rawEvent(line, source, receivedAt)
		e.ParseError = true
		return e, dialectRaw
	case dialectRaw:
		return p.rawEvent(line, source, receivedAt), dialectRaw
	default:
		return p.rawEvent(line, source, receivedAt), dialectRaw
	}
}

func detectDialect(line string) string {
	end := strings.IndexByte(line, '>')
	if len(line) == 0 || line[0] != '<' || end <= 1 {
		return dialectRaw
	}
	after := end + 1
	if after+1 < len(line) && line[after] >= '1' && line[after] <= '9' && line[after+1] == ' ' {
		return dialectRFC5424
	}
	return dialectRFC3164
}

func (p *parser) rawEvent(line, source string, receivedAt time.Time) *event.Event {
	e := event.NewEvent(receivedAt, line)
	e.Source = source
	e.SourceType = p.sourceType(dialectRaw)
	e.Index = p.cfg.Index
	if p.cfg.DefaultHostname != "" {
		e.Host = p.cfg.DefaultHostname
	}
	return e
}

func (p *parser) parseRFC5424(line, source string, receivedAt time.Time) (*event.Event, error) {
	pri, rest, err := parsePRI(line)
	if err != nil {
		return nil, err
	}
	tokens := splitN(rest, ' ', 7)
	if len(tokens) != 7 {
		return nil, fmt.Errorf("rfc5424: too few header fields")
	}
	if tokens[0] == "" || tokens[0][0] < '1' || tokens[0][0] > '9' {
		return nil, fmt.Errorf("rfc5424: invalid version")
	}

	ts := receivedAt
	if tokens[1] != "-" {
		parsed, parseErr := time.Parse(time.RFC3339Nano, tokens[1])
		if parseErr != nil {
			return nil, parseErr
		}
		ts = parsed
	}

	e := event.NewEvent(ts, line)
	e.Source = source
	e.SourceType = p.sourceType(dialectRFC5424)
	e.Index = p.cfg.Index
	e.Host = nilValue(tokens[2], p.cfg.DefaultHostname)
	setPRIFields(e, pri)
	setIfNotNil(e, "app_name", tokens[3])
	setIfNotNil(e, "procid", tokens[4])
	setIfNotNil(e, "msgid", tokens[5])

	sd, msg, err := splitStructuredDataAndMessage(tokens[6])
	if err != nil {
		return nil, err
	}
	parseStructuredData(e, sd)
	if strings.HasPrefix(msg, "\xef\xbb\xbf") {
		msg = strings.TrimPrefix(msg, "\xef\xbb\xbf")
	}
	e.SetField("message", event.StringValue(msg))

	return e, nil
}

func (p *parser) parseRFC3164(line, source string, receivedAt time.Time) (*event.Event, error) {
	pri, rest, err := parsePRI(line)
	if err != nil {
		return nil, err
	}
	if len(rest) < 16 {
		return nil, fmt.Errorf("rfc3164: too short")
	}

	tsText := rest[:15]
	loc := p.cfg.Location
	ts, err := time.ParseInLocation("Jan _2 15:04:05", tsText, loc)
	if err != nil {
		return nil, err
	}
	ts = time.Date(receivedAt.In(loc).Year(), ts.Month(), ts.Day(), ts.Hour(), ts.Minute(), ts.Second(), 0, loc)

	remain := strings.TrimSpace(rest[15:])
	host, remain, ok := cutToken(remain)
	if !ok {
		return nil, fmt.Errorf("rfc3164: missing host")
	}

	app := ""
	procid := ""
	msg := remain
	if tag, body, found := strings.Cut(remain, ":"); found {
		msg = strings.TrimLeft(body, " ")
		app = tag
		if i := strings.IndexByte(app, '['); i >= 0 && strings.HasSuffix(app, "]") {
			procid = strings.TrimSuffix(app[i+1:], "]")
			app = app[:i]
		}
	} else if tag, body, found := strings.Cut(remain, " "); found {
		app = tag
		msg = strings.TrimLeft(body, " ")
	}

	e := event.NewEvent(ts, line)
	e.Source = source
	e.SourceType = p.sourceType(dialectRFC3164)
	e.Index = p.cfg.Index
	e.Host = nilValue(host, p.cfg.DefaultHostname)
	setPRIFields(e, pri)
	setIfNotNil(e, "app_name", app)
	setIfNotNil(e, "procid", procid)
	e.SetField("message", event.StringValue(msg))

	return e, nil
}

func parsePRI(line string) (int, string, error) {
	if !strings.HasPrefix(line, "<") {
		return 0, "", fmt.Errorf("missing pri")
	}
	end := strings.IndexByte(line, '>')
	if end <= 1 {
		return 0, "", fmt.Errorf("missing pri terminator")
	}
	pri, err := strconv.Atoi(line[1:end])
	if err != nil || pri < 0 || pri > 191 {
		return 0, "", fmt.Errorf("invalid pri")
	}
	return pri, line[end+1:], nil
}

func setPRIFields(e *event.Event, pri int) {
	facility := pri / 8
	severity := pri % 8
	e.SetField("facility", event.IntValue(int64(facility)))
	e.SetField("severity", event.IntValue(int64(severity)))
	if facility >= 0 && facility < len(facilityLabels) {
		e.SetField("facility_label", event.StringValue(facilityLabels[facility]))
	}
	if severity >= 0 && severity < len(severityLabels) {
		label := severityLabels[severity]
		e.SetField("severity_label", event.StringValue(label))
		e.SetField("level", event.StringValue(label))
	}
}

func splitStructuredDataAndMessage(s string) (string, string, error) {
	if s == "" {
		return "", "", nil
	}
	if s[0] == '-' {
		if len(s) == 1 {
			return "-", "", nil
		}
		if s[1] != ' ' {
			return "", "", fmt.Errorf("rfc5424: invalid nil structured data")
		}
		return "-", s[2:], nil
	}
	if s[0] != '[' {
		return "", "", fmt.Errorf("rfc5424: missing structured data")
	}
	depth := 0
	escaped := false
	for i, r := range s {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		switch r {
		case '[':
			depth++
		case ']':
			depth--
			if depth == 0 {
				if i+1 == len(s) {
					return s[:i+1], "", nil
				}
				if s[i+1] == ' ' {
					return s[:i+1], s[i+2:], nil
				}
				if s[i+1] == '[' {
					continue
				}
				return "", "", fmt.Errorf("rfc5424: invalid structured data separator")
			}
		}
	}
	return "", "", fmt.Errorf("rfc5424: unterminated structured data")
}

func parseStructuredData(e *event.Event, sd string) {
	if sd == "" || sd == "-" {
		return
	}
	for _, elem := range splitSDElements(sd) {
		inner := strings.Trim(elem, "[]")
		id, params, _ := strings.Cut(inner, " ")
		id = sanitizeSDKey(id)
		for len(params) > 0 {
			params = strings.TrimLeft(params, " ")
			key, rest, ok := cutUntil(params, '=')
			if !ok || len(rest) < 2 || rest[0] != '"' {
				return
			}
			val, rem, ok := readQuoted(rest)
			if !ok {
				return
			}
			e.SetField("sd_"+id+"_"+sanitizeSDKey(key), event.StringValue(val))
			params = rem
		}
	}
}

func splitSDElements(sd string) []string {
	var elems []string
	start := -1
	depth := 0
	escaped := false
	for i, r := range sd {
		if escaped {
			escaped = false
			continue
		}
		if r == '\\' {
			escaped = true
			continue
		}
		if r == '[' {
			if depth == 0 {
				start = i
			}
			depth++
		}
		if r == ']' {
			depth--
			if depth == 0 && start >= 0 {
				elems = append(elems, sd[start:i+1])
			}
		}
	}
	return elems
}

func readQuoted(s string) (string, string, bool) {
	var b strings.Builder
	for i := 1; i < len(s); {
		r, size := utf8.DecodeRuneInString(s[i:])
		if r == utf8.RuneError && size == 1 {
			return "", "", false
		}
		if r == '"' {
			return b.String(), s[i+size:], true
		}
		if r == '\\' {
			if i+size >= len(s) {
				return "", "", false
			}
			r, size = utf8.DecodeRuneInString(s[i+size:])
		}
		b.WriteRune(r)
		i += size
	}
	return "", "", false
}

func (p *parser) sourceType(dialect string) string {
	base := p.cfg.SourceType
	if base == "" {
		base = "syslog"
	}
	return base + ":" + dialect
}

func splitN(s string, sep byte, n int) []string {
	var out []string
	for len(out) < n-1 {
		i := strings.IndexByte(s, sep)
		if i < 0 {
			return out
		}
		out = append(out, s[:i])
		s = s[i+1:]
	}
	out = append(out, s)
	return out
}

func cutToken(s string) (string, string, bool) {
	s = strings.TrimLeft(s, " ")
	if s == "" {
		return "", "", false
	}
	i := strings.IndexByte(s, ' ')
	if i < 0 {
		return s, "", true
	}
	return s[:i], strings.TrimLeft(s[i+1:], " "), true
}

func cutUntil(s string, delim byte) (string, string, bool) {
	i := strings.IndexByte(s, delim)
	if i < 0 {
		return "", "", false
	}
	return s[:i], s[i+1:], true
}

func nilValue(v, fallback string) string {
	if v == "" || v == "-" {
		return fallback
	}
	return v
}

func setIfNotNil(e *event.Event, key, value string) {
	if value == "" || value == "-" {
		return
	}
	e.SetField(key, event.StringValue(value))
}

func sanitizeSDKey(s string) string {
	var b strings.Builder
	for _, r := range s {
		if unicode.IsLetter(r) || unicode.IsDigit(r) || r == '_' {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	return strings.Trim(b.String(), "_")
}

func locationForConfig(cfg config.SyslogConfig) (*time.Location, error) {
	if cfg.DefaultTimezone == "" || cfg.DefaultTimezone == "Local" {
		return time.Local, nil
	}
	return time.LoadLocation(cfg.DefaultTimezone)
}
