package handlers

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
)

func testJob(jobType, payload string) domain.Job {
	return domain.Job{
		JobType: jobType,
		Payload: json.RawMessage(payload),
	}
}

func decodeResult(t *testing.T, result json.RawMessage) map[string]any {
	t.Helper()
	var decoded map[string]any
	if err := json.Unmarshal(result, &decoded); err != nil {
		t.Fatalf("decode result: %v", err)
	}
	return decoded
}

func TestReportGenerateHandler(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"valid", `{"report_id":"r-1","format":"csv"}`, ""},
		{"default format", `{"report_id":"r-2"}`, ""},
		{"missing report id", `{"format":"pdf"}`, "report_id"},
		{"invalid json", `{`, "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := (ReportGenerateHandler{}).Execute(context.Background(), testJob("report.generate", tt.payload))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Execute() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			decoded := decodeResult(t, result)
			if decoded["status"] != "generated" {
				t.Errorf("status = %v, want generated", decoded["status"])
			}
			if tt.name == "default format" && decoded["format"] != "pdf" {
				t.Errorf("format = %v, want pdf", decoded["format"])
			}
		})
	}
}

func TestEmailSendHandler(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"valid", `{"to":"person@example.com","template":"welcome"}`, ""},
		{"missing to", `{}`, "to address"},
		{"invalid to", `{"to":"not-an-email"}`, "to address"},
		{"invalid json", `{`, "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := (EmailSendHandler{}).Execute(context.Background(), testJob("email.send", tt.payload))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Execute() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			decoded := decodeResult(t, result)
			if decoded["status"] != "sent" {
				t.Errorf("status = %v, want sent", decoded["status"])
			}
			if decoded["to"] != "person@example.com" {
				t.Errorf("to = %v, want person@example.com", decoded["to"])
			}
		})
	}
}

func TestDataImportHandler(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"valid", `{"source":"s3://bucket/key","format":"csv"}`, ""},
		{"missing source", `{"format":"csv"}`, "source"},
		{"invalid json", `{`, "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result, err := (DataImportHandler{}).Execute(context.Background(), testJob("data.import", tt.payload))
			if tt.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
					t.Fatalf("Execute() error = %v, want containing %q", err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("Execute() error = %v", err)
			}
			decoded := decodeResult(t, result)
			if decoded["status"] != "imported" {
				t.Errorf("status = %v, want imported", decoded["status"])
			}
		})
	}
}

func TestWebhookDeliverHandlerSuccess(t *testing.T) {
	var (
		gotMethod      string
		gotContentType string
		gotHeader      string
		gotBody        string
	)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotMethod = r.Method
		gotContentType = r.Header.Get("Content-Type")
		gotHeader = r.Header.Get("X-Test")
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	payload, err := json.Marshal(map[string]any{
		"url":     server.URL,
		"body":    map[string]any{"event": "created"},
		"headers": map[string]string{"X-Test": "yes"},
	})
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	result, err := (&WebhookDeliverHandler{}).Execute(context.Background(), testJob("webhook.deliver", string(payload)))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	decoded := decodeResult(t, result)
	if decoded["delivered"] != true {
		t.Errorf("delivered = %v, want true", decoded["delivered"])
	}
	if decoded["status_code"] != float64(http.StatusNoContent) {
		t.Errorf("status_code = %v, want %d", decoded["status_code"], http.StatusNoContent)
	}
	if gotMethod != http.MethodPost {
		t.Errorf("method = %q, want POST", gotMethod)
	}
	if gotContentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", gotContentType)
	}
	if gotHeader != "yes" {
		t.Errorf("X-Test = %q, want yes", gotHeader)
	}
	if gotBody != `{"event":"created"}` {
		t.Errorf("body = %s, want {\"event\":\"created\"}", gotBody)
	}
}

func TestWebhookDeliverHandlerDefaultsBody(t *testing.T) {
	var gotBody string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		gotBody = string(body)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	result, err := (&WebhookDeliverHandler{}).Execute(context.Background(), testJob("webhook.deliver", `{"url":"`+server.URL+`"}`))
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if gotBody != `{}` {
		t.Errorf("body = %q, want {}", gotBody)
	}
	if decodeResult(t, result)["delivered"] != true {
		t.Error("delivered = false, want true")
	}
}

func TestWebhookDeliverHandlerFailureIsSanitized(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("secret upstream details"))
	}))
	defer server.Close()

	_, err := (&WebhookDeliverHandler{}).Execute(context.Background(), testJob("webhook.deliver", `{"url":"`+server.URL+`"}`))
	if err == nil {
		t.Fatal("Execute() error = nil, want error")
	}
	if !strings.Contains(err.Error(), "HTTP 500") {
		t.Errorf("error = %q, want HTTP 500", err)
	}
	if strings.Contains(err.Error(), "secret upstream details") {
		t.Errorf("error leaked response body: %q", err)
	}
}

func TestWebhookDeliverHandlerValidation(t *testing.T) {
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"missing url", `{}`, "valid url"},
		{"unsupported scheme", `{"url":"ftp://example.com/hook"}`, "http or https"},
		{"invalid outer json", `{"url":"https://example.com/hook","body":{`, "decode"},
		{"invalid json", `{`, "decode"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := (&WebhookDeliverHandler{}).Execute(context.Background(), testJob("webhook.deliver", tt.payload))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("Execute() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}
}

func TestWebhookDeliverHandlerTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(100 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	handler := &WebhookDeliverHandler{Client: &http.Client{Timeout: 20 * time.Millisecond}}
	_, err := handler.Execute(context.Background(), testJob("webhook.deliver", `{"url":"`+server.URL+`"}`))
	if err == nil || !strings.Contains(err.Error(), "request failed") {
		t.Fatalf("Execute() error = %v, want sanitized request failure", err)
	}
}
