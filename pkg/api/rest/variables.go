package rest

import (
	"sort"
	"strings"
)

// substituteVariables replaces $key tokens in the query with quoted, escaped
// values from the provided variables map. Keys are sorted by length descending
// so that $source_type is replaced before $source. Values are wrapped in
// double quotes with internal double quotes escaped via backslash.
func substituteVariables(query string, vars map[string]string) string {
	if len(vars) == 0 {
		return query
	}

	// Collect and sort keys by length descending to prevent partial matches.
	keys := make([]string, 0, len(vars))
	for k := range vars {
		keys = append(keys, k)
	}
	sort.Slice(keys, func(i, j int) bool {
		return len(keys[i]) > len(keys[j])
	})

	for _, k := range keys {
		escaped := strings.ReplaceAll(vars[k], `\`, `\\`)
		escaped = strings.ReplaceAll(escaped, `"`, `\"`)
		query = strings.ReplaceAll(query, "$"+k, `"`+escaped+`"`)
	}

	return query
}
