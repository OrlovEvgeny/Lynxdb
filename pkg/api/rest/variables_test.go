package rest

import "testing"

func TestSubstituteVariables(t *testing.T) {
	tests := []struct {
		name  string
		query string
		vars  map[string]string
		want  string
	}{
		{
			name:  "nil vars returns query unchanged",
			query: "source=$source | stats count",
			vars:  nil,
			want:  "source=$source | stats count",
		},
		{
			name:  "empty vars returns query unchanged",
			query: "source=$source | stats count",
			vars:  map[string]string{},
			want:  "source=$source | stats count",
		},
		{
			name:  "basic substitution",
			query: "source=$source | stats count",
			vars:  map[string]string{"source": "nginx"},
			want:  `source="nginx" | stats count`,
		},
		{
			name:  "multiple variables",
			query: "source=$source level=$level | stats count",
			vars:  map[string]string{"source": "nginx", "level": "error"},
			want:  `source="nginx" level="error" | stats count`,
		},
		{
			name:  "injection protection escapes quotes",
			query: "source=$source | stats count",
			vars:  map[string]string{"source": `error" OR 1=1`},
			want:  `source="error\" OR 1=1" | stats count`,
		},
		{
			name:  "overlapping variable names sorted by length",
			query: "source=$src_type host=$src",
			vars:  map[string]string{"src": "web-01", "src_type": "nginx"},
			want:  `source="nginx" host="web-01"`,
		},
		{
			name:  "variable appears multiple times",
			query: "$env and source=$env",
			vars:  map[string]string{"env": "prod"},
			want:  `"prod" and source="prod"`,
		},
		{
			name:  "no matching variables",
			query: "source=nginx | stats count",
			vars:  map[string]string{"host": "web-01"},
			want:  "source=nginx | stats count",
		},
		{
			name:  "value with backslash",
			query: "path=$path",
			vars:  map[string]string{"path": `C:\logs\app`},
			want:  `path="C:\\logs\\app"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := substituteVariables(tt.query, tt.vars)
			if got != tt.want {
				t.Errorf("substituteVariables() =\n  %q\nwant\n  %q", got, tt.want)
			}
		})
	}
}
