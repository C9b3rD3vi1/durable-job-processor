package handlers

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
)

func TestBuiltinPayloadValidation(t *testing.T) {
	tests := []struct {
		name    string
		jobType string
		payload string
		wantErr bool
	}{
		{"report valid", "report.generate", `{"report_id":"r-1"}`, false},
		{"report default format", "report.generate", `{"report_id":"r-1","format":"  "}`, false},
		{"report missing id", "report.generate", `{}`, true},
		{"report empty id", "report.generate", `{"report_id":"   "}`, true},
		{"email valid", "email.send", `{"to":"user@example.com"}`, false},
		{"email missing address", "email.send", `{"to":"not-an-address"}`, true},
		{"import valid", "data.import", `{"source":"s3://bucket"}`, false},
		{"import missing source", "data.import", `{}`, true},
		{"webhook valid", "webhook.deliver", `{"url":"https://example.com/hook","body":{"ok":true}}`, false},
		{"webhook missing url", "webhook.deliver", `{}`, true},
		{"webhook invalid scheme", "webhook.deliver", `{"url":"file:///tmp/hook"}`, true},
		{"webhook invalid body", "webhook.deliver", `{"url":"https://example.com/hook","body":{`, true},
	}

	registry := DefaultRegistry()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := registry.ValidatePayload(tt.jobType, json.RawMessage(tt.payload))
			if (err != nil) != tt.wantErr {
				t.Fatalf("ValidatePayload(%q) error = %v, wantErr=%v", tt.jobType, err, tt.wantErr)
			}
		})
	}
}

func TestPayloadSchemaIsSharedWithExecution(t *testing.T) {
	// The same validation helper must reject a bad payload both at submission
	// and at execution, so jobs already in the queue stay consistent.
	payload := json.RawMessage(`{}`)
	registry := DefaultRegistry()
	if err := registry.ValidatePayload("email.send", payload); err == nil {
		t.Fatal("ValidatePayload() accepted an email without a recipient")
	}

	handler, ok := registry.Handler("email.send")
	if !ok {
		t.Fatal("email handler is not registered")
	}
	result, err := handler.Execute(context.Background(), domain.Job{
		JobType: "email.send",
		Payload: payload,
	})
	if err == nil || result != nil {
		t.Fatalf("Execute() = %s, %v; want an error and no result", result, err)
	}
}

func TestRegistryPayloadValidationForHandlersWithoutSchema(t *testing.T) {
	registry := New()
	err := registry.Register("custom.job", HandlerFunc(
		func(context.Context, domain.Job) (json.RawMessage, error) {
			return nil, nil
		},
	))
	if err != nil {
		t.Fatalf("Register() error = %v", err)
	}
	if err := registry.ValidatePayload("custom.job", json.RawMessage(`{"anything":true}`)); err != nil {
		t.Fatalf("ValidatePayload() error = %v, want nil for handlers without a schema", err)
	}
	if err := registry.ValidatePayload("unknown.job", json.RawMessage(`{}`)); err == nil {
		t.Fatal("ValidatePayload() accepted an unregistered job type")
	}
}
