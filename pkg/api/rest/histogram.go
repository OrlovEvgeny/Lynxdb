package rest

import (
	"net/http"
	"time"

	"github.com/lynxbase/lynxdb/pkg/usecases"
)

func (s *Server) handleHistogram(w http.ResponseWriter, r *http.Request) {
	start := time.Now()

	fromStr := r.URL.Query().Get("from")
	toStr := r.URL.Query().Get("to")

	bucketCount := parseIntParam(r, "buckets", 60)
	if bucketCount <= 0 {
		bucketCount = 60
	}
	if bucketCount > 1000 {
		bucketCount = 1000
	}

	groupBy := r.URL.Query().Get("group_by")

	req := usecases.HistogramRequest{
		From:    fromStr,
		To:      toStr,
		Buckets: bucketCount,
		Index:   r.URL.Query().Get("index"),
		GroupBy: groupBy,
	}

	// Grouped histogram path: returns counts broken down by field value.
	if groupBy != "" {
		grouped, err := s.queryService.GroupedHistogram(r.Context(), req)
		if err != nil {
			if isValidationError(err) {
				respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
			} else {
				respondInternalError(w, err.Error())
			}
			return
		}

		buckets := make([]map[string]interface{}, len(grouped.Buckets))
		for i, b := range grouped.Buckets {
			buckets[i] = map[string]interface{}{
				"time":   b.Time.UTC().Format(time.RFC3339),
				"counts": b.Counts,
			}
		}

		took := time.Since(start)
		respondData(w, http.StatusOK, map[string]interface{}{
			"interval": grouped.Interval,
			"buckets":  buckets,
			"total":    grouped.Total,
		}, WithTook(took))
		return
	}

	// Ungrouped histogram path: backward compatible.
	result, err := s.queryService.Histogram(r.Context(), req)
	if err != nil {
		if isValidationError(err) {
			respondError(w, ErrCodeValidationError, http.StatusBadRequest, err.Error())
		} else {
			respondInternalError(w, err.Error())
		}
		return
	}

	buckets := make([]map[string]interface{}, len(result.Buckets))
	for i, b := range result.Buckets {
		buckets[i] = map[string]interface{}{
			"time":  b.Time.UTC().Format(time.RFC3339),
			"count": b.Count,
		}
	}

	took := time.Since(start)
	respondData(w, http.StatusOK, map[string]interface{}{
		"interval": result.Interval,
		"buckets":  buckets,
		"total":    result.Total,
	}, WithTook(took))
}
