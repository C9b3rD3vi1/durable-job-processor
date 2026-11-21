package worker

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/handlers"
	"github.com/C9b3rD3vi1/durable-job-processor/internal/observability"
)

func TestPublishQueueStats(t *testing.T) {
	metrics := observability.NewMetrics()
	store := &fakeStore{jobs: []*domain.Job{{}, {}, {}}}
	runner, err := New(Config{
		Store:    store,
		Registry: handlers.DefaultRegistry(),
		WorkerID: "stats-test",
		Metrics:  metrics,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}

	runner.publishQueueStats(context.Background())

	recorder := httptest.NewRecorder()
	metrics.Handler("").ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body := recorder.Body.String()
	for _, want := range []string{"jobs_queue_depth 3", "jobs_oldest_due_seconds 1.5"} {
		if !strings.Contains(body, want) {
			t.Errorf("metrics missing %q\n%s", want, body)
		}
	}
}
