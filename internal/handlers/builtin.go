package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/domain"
)

const (
	webhookTimeout       = 10 * time.Second
	webhookResponseLimit = 64 * 1024
)

// DefaultRegistry registers the job types supported by this implementation.
// The API uses it as an allow-list; workers use the same registry to dispatch.
func DefaultRegistry() *Registry {
	registry := New()
	builtins := map[string]Handler{
		"report.generate": ReportGenerateHandler{},
		"email.send":      EmailSendHandler{},
		"webhook.deliver": &WebhookDeliverHandler{Client: &http.Client{Timeout: webhookTimeout}},
		"data.import":     DataImportHandler{},
	}
	for jobType, handler := range builtins {
		if err := registry.Register(jobType, handler); err != nil {
			panic(fmt.Sprintf("register built-in handler %s: %v", jobType, err))
		}
	}
	return registry
}

// ReportGenerateHandler simulates report generation.
type ReportGenerateHandler struct{}

type reportGeneratePayload struct {
	ReportID string `json:"report_id"`
	Format   string `json:"format"`
}

// Execute validates the payload and returns a bounded report result.
func (ReportGenerateHandler) Execute(_ context.Context, job domain.Job) (json.RawMessage, error) {
	var payload reportGeneratePayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode report.generate payload: %w", err)
	}
	payload.ReportID = strings.TrimSpace(payload.ReportID)
	if payload.ReportID == "" {
		return nil, errors.New("report.generate requires report_id")
	}
	if payload.Format = strings.TrimSpace(payload.Format); payload.Format == "" {
		payload.Format = "pdf"
	}

	return json.Marshal(map[string]any{
		"report_id": payload.ReportID,
		"format":    payload.Format,
		"status":    "generated",
	})
}

// EmailSendHandler simulates email delivery.
type EmailSendHandler struct{}

type emailSendPayload struct {
	To       string `json:"to"`
	Template string `json:"template"`
}

// Execute validates the recipient and returns a delivery result.
func (EmailSendHandler) Execute(_ context.Context, job domain.Job) (json.RawMessage, error) {
	var payload emailSendPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode email.send payload: %w", err)
	}
	payload.To = strings.TrimSpace(payload.To)
	if payload.To == "" || !strings.Contains(payload.To, "@") {
		return nil, errors.New("email.send requires a valid to address")
	}

	return json.Marshal(map[string]any{
		"to":        payload.To,
		"template":  payload.Template,
		"status":    "sent",
		"deliverer": "simulator",
	})
}

// DataImportHandler simulates a data import.
type DataImportHandler struct{}

type dataImportPayload struct {
	Source string `json:"source"`
	Format string `json:"format"`
}

// Execute validates the source and returns an import result.
func (DataImportHandler) Execute(_ context.Context, job domain.Job) (json.RawMessage, error) {
	var payload dataImportPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode data.import payload: %w", err)
	}
	payload.Source = strings.TrimSpace(payload.Source)
	if payload.Source == "" {
		return nil, errors.New("data.import requires source")
	}

	return json.Marshal(map[string]any{
		"source": payload.Source,
		"format": payload.Format,
		"status": "imported",
	})
}

// WebhookDeliverHandler POSTs a JSON body to a caller-supplied URL.
type WebhookDeliverHandler struct {
	Client *http.Client
}

type webhookDeliverPayload struct {
	URL     string            `json:"url"`
	Body    json.RawMessage   `json:"body,omitempty"`
	Headers map[string]string `json:"headers,omitempty"`
}

// Execute performs the webhook request and requires a 2xx response.
func (h *WebhookDeliverHandler) Execute(ctx context.Context, job domain.Job) (json.RawMessage, error) {
	var payload webhookDeliverPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return nil, fmt.Errorf("decode webhook.deliver payload: %w", err)
	}

	target, err := url.Parse(strings.TrimSpace(payload.URL))
	if err != nil || target.Host == "" {
		return nil, errors.New("webhook.deliver requires a valid url")
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return nil, errors.New("webhook.deliver requires an http or https url")
	}

	body := bytes.TrimSpace(payload.Body)
	if len(body) == 0 {
		body = json.RawMessage(`{}`)
	}
	if !json.Valid(body) {
		return nil, errors.New("webhook.deliver body must be valid JSON")
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, target.String(), bytes.NewReader(body))
	if err != nil {
		return nil, errors.New("webhook.deliver could not build request")
	}
	req.Header.Set("Content-Type", "application/json")
	for name, value := range payload.Headers {
		req.Header.Set(name, value)
	}

	client := h.Client
	if client == nil {
		client = &http.Client{Timeout: webhookTimeout}
	}

	resp, err := client.Do(req)
	if err != nil {
		return nil, errors.New("webhook.deliver request failed")
	}
	defer func() { _ = resp.Body.Close() }()
	_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, webhookResponseLimit))

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return nil, fmt.Errorf("webhook.deliver returned HTTP %d", resp.StatusCode)
	}

	return json.Marshal(map[string]any{
		"delivered":   true,
		"status_code": resp.StatusCode,
	})
}
