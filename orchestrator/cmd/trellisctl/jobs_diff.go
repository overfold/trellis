package main

import (
	"encoding/json"
	"fmt"
	"io"
	"strings"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
)

func printJobPlan(w io.Writer, result *api.JobPlanResponse) error {
	if config.Output == "json" {
		return writeJSON(w, result)
	}

	switch result.Action {
	case "create":
		_, err := fmt.Fprintf(w, "Plan: create %s/%s (%d desired allocations)\n", result.Namespace, result.Job, result.DesiredAllocations)
		return err
	case "none":
		_, err := fmt.Fprintf(w, "Plan: no changes to %s/%s (version %d, revision %d)\n", result.Namespace, result.Job, result.BaseVersion, result.BaseRevision)
		return err
	default:
		if _, err := fmt.Fprintf(w, "Plan: update %s/%s from version %d (revision %d)\n", result.Namespace, result.Job, result.BaseVersion, result.BaseRevision); err != nil {
			return err
		}
		for _, change := range result.Changes {
			switch change.Operation {
			case "add":
				if _, err := fmt.Fprintf(w, "  + %s: %s\n", change.Path, formatChangeValue(change.Path, change.After)); err != nil {
					return err
				}
			case "remove":
				if _, err := fmt.Fprintf(w, "  - %s: %s\n", change.Path, formatChangeValue(change.Path, change.Before)); err != nil {
					return err
				}
			default:
				if _, err := fmt.Fprintf(w, "  ~ %s: %s -> %s\n", change.Path, formatChangeValue(change.Path, change.Before), formatChangeValue(change.Path, change.After)); err != nil {
					return err
				}
			}
		}
		return nil
	}
}

func formatChangeValue(path string, value any) string {
	if value == nil {
		return "null"
	}
	if number, ok := value.(float64); ok && isDurationPath(path) {
		return time.Duration(int64(number)).String()
	}
	if number, ok := value.(json.Number); ok && isDurationPath(path) {
		if nanos, err := number.Int64(); err == nil {
			return time.Duration(nanos).String()
		}
	}
	switch typed := value.(type) {
	case string:
		return fmt.Sprintf("%q", typed)
	case float64:
		if typed == float64(int64(typed)) {
			return fmt.Sprintf("%d", int64(typed))
		}
		return fmt.Sprintf("%v", typed)
	case bool:
		return fmt.Sprintf("%t", typed)
	default:
		raw, err := json.Marshal(typed)
		if err != nil {
			return fmt.Sprintf("%v", typed)
		}
		return string(raw)
	}
}

func isDurationPath(path string) bool {
	return strings.HasSuffix(path, ".window") || strings.HasSuffix(path, ".interval") || strings.HasSuffix(path, ".timeout")
}
