package shell

import (
	"encoding/json"

	"github.com/lynxbase/lynxdb/pkg/client"
)

// queryResultToRows converts a typed *client.QueryResult to a flat row slice.
// Duplicated from cmd/lynxdb/query.go because cmd/ is not importable from internal/.
func queryResultToRows(result *client.QueryResult) []map[string]interface{} {
	if result == nil {
		return nil
	}

	switch result.Type {
	case client.ResultTypeEvents:
		if result.Events != nil {
			return result.Events.Events
		}
	case client.ResultTypeAggregate, client.ResultTypeTimechart:
		if result.Aggregate != nil {
			rows := make([]map[string]interface{}, 0, len(result.Aggregate.Rows))
			for _, row := range result.Aggregate.Rows {
				m := make(map[string]interface{}, len(result.Aggregate.Columns))
				for j, col := range result.Aggregate.Columns {
					if j < len(row) {
						m[col] = row[j]
					}
				}

				rows = append(rows, m)
			}

			return rows
		}
	}

	return nil
}

// jobResultToRows extracts rows from a completed async job result.
// The job.Results field is a *json.RawMessage that must be unmarshalled
// into a typed result structure.
func jobResultToRows(job *client.JobResult) []map[string]interface{} {
	if job == nil || job.Results == nil {
		return nil
	}

	// Unmarshal the raw results into a typed envelope.
	var envelope struct {
		Type    string                   `json:"type"`
		Events  []map[string]interface{} `json:"events,omitempty"`
		Columns []string                 `json:"columns,omitempty"`
		Rows    [][]interface{}          `json:"rows,omitempty"`
	}
	if err := json.Unmarshal(*job.Results, &envelope); err != nil {
		return nil
	}

	switch envelope.Type {
	case "events":
		return envelope.Events
	case "aggregate", "timechart":
		rows := make([]map[string]interface{}, 0, len(envelope.Rows))
		for _, row := range envelope.Rows {
			m := make(map[string]interface{}, len(envelope.Columns))
			for j, col := range envelope.Columns {
				if j < len(row) {
					m[col] = row[j]
				}
			}

			rows = append(rows, m)
		}

		return rows
	}

	return nil
}
