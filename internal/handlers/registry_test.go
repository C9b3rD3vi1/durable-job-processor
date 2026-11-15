package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
)

func stubHandler() Handler {
	return HandlerFunc(func(context.Context, domain.Job) (json.RawMessage, error) {
		return json.RawMessage(`{"ok":true}`), nil
	})
}

func TestDefaultRegistry(t *testing.T) {
	registry := DefaultRegistry()
	for _, jobType := range []string{"report.generate", "email.send", "webhook.deliver", "data.import"} {
		if !registry.Allows(jobType) {
			t.Errorf("Allows(%q) = false, want true", jobType)
		}
		if handler, ok := registry.Handler(jobType); !ok || handler == nil {
			t.Errorf("Handler(%q) = %v, %v; want non-nil handler", jobType, handler, ok)
		}
	}
	if registry.Allows("unknown.job") {
		t.Error(`Allows("unknown.job") = true, want false`)
	}
	if _, ok := registry.Handler("unknown.job"); ok {
		t.Error(`Handler("unknown.job") reported a handler`)
	}
}

func TestRegisterRejectsInvalidDefinitions(t *testing.T) {
	tests := []struct {
		name    string
		jobType string
		handler Handler
	}{
		{"empty type", "   ", stubHandler()},
		{"nil handler", "report.generate", nil},
		{"type too long", strings.Repeat("a", domain.MaxJobTypeLength+1), stubHandler()},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			registry := New()
			if err := registry.Register(tt.jobType, tt.handler); err == nil {
				t.Fatal("Register() error = nil, want error")
			}
		})
	}
}

func TestRegisterRejectsDuplicates(t *testing.T) {
	registry := New()
	if err := registry.Register("report.generate", stubHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	err := registry.Register("report.generate", stubHandler())
	if !errors.Is(err, ErrHandlerRegistered) {
		t.Fatalf("Register() error = %v, want ErrHandlerRegistered", err)
	}
}

func TestRegisterTrimsJobType(t *testing.T) {
	registry := New()
	if err := registry.Register("  report.generate  ", stubHandler()); err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if !registry.Allows("report.generate") {
		t.Error("trimmed job type was not registered")
	}
}

func TestRegistryTypesSorted(t *testing.T) {
	registry := New()
	for _, jobType := range []string{"z.job", "a.job", "m.job"} {
		if err := registry.Register(jobType, stubHandler()); err != nil {
			t.Fatalf("Register(%q) error = %v", jobType, err)
		}
	}

	want := []string{"a.job", "m.job", "z.job"}
	types := registry.Types()
	if len(types) != len(want) {
		t.Fatalf("Types() = %v, want %v", types, want)
	}
	for i := range want {
		if types[i] != want[i] {
			t.Fatalf("Types() = %v, want %v", types, want)
		}
	}
}

func TestNilRegistry(t *testing.T) {
	var registry *Registry
	if registry.Allows("report.generate") {
		t.Error("nil registry should not allow jobs")
	}
	if _, ok := registry.Handler("report.generate"); ok {
		t.Error("nil registry should not return a handler")
	}
	if registry.Types() != nil {
		t.Error("nil registry Types() should be nil")
	}
	if err := registry.Register("report.generate", stubHandler()); err == nil {
		t.Error("nil registry Register() error = nil, want error")
	}
}

func TestHandlerFunc(t *testing.T) {
	called := false
	handler := HandlerFunc(func(_ context.Context, job domain.Job) (json.RawMessage, error) {
		called = true
		if job.JobType != "report.generate" {
			t.Fatalf("job type = %q", job.JobType)
		}
		return json.RawMessage(`{"ok":true}`), nil
	})

	result, err := handler.Execute(context.Background(), domain.Job{JobType: "report.generate"})
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !called {
		t.Error("handler was not called")
	}
	if string(result) != `{"ok":true}` {
		t.Errorf("result = %s, want {\"ok\":true}", result)
	}
}
