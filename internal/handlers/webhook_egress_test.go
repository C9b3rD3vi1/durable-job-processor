package handlers

import (
	"context"
	"errors"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/C9b3rD3vi1/durable-job-processor/internal/retry"
)

func TestWebhookEgressBlocksNonPublicLiteralAddresses(t *testing.T) {
	tests := []struct {
		name string
		url  string
	}{
		{"loopback v4", "http://127.0.0.1:8080/hook"},
		{"loopback v6", "http://[::1]:8080/hook"},
		{"private v4", "http://10.1.2.3/hook"},
		{"private v6", "http://[fd00::1]/hook"},
		{"metadata service", "http://169.254.169.254/latest/meta-data/"},
		{"link local v6", "http://[fe80::1]/hook"},
		{"carrier nat", "http://100.64.0.1/hook"},
		{"documentation", "http://192.0.2.1/hook"},
		{"multicast", "http://224.0.0.1/hook"},
	}
	handler := NewWebhookDeliverHandler(WebhookEgressPolicy{})
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload := `{"url":"` + tt.url + `"}`
			if err := handler.ValidatePayload([]byte(payload)); err == nil || !strings.Contains(err.Error(), "not permitted") {
				t.Fatalf("ValidatePayload() error = %v, want not permitted", err)
			}
			if _, err := handler.Execute(context.Background(), testJob("webhook.deliver", payload)); err == nil {
				t.Error("Execute() error = nil, want rejection")
			}
		})
	}
}

func TestWebhookEgressAllowPrivateIPsStillRejectsNonPeerAddresses(t *testing.T) {
	policy := WebhookEgressPolicy{AllowPrivateIPs: true}
	for _, address := range []string{"0.0.0.0", "::", "224.0.0.1", "ff02::1"} {
		t.Run(address, func(t *testing.T) {
			ip := net.ParseIP(address)
			if ip == nil {
				t.Fatalf("ParseIP(%q) = nil", address)
			}
			if policy.allowsIP(ip) {
				t.Fatalf("allowsIP(%s) = true, want false for a non-peer address", address)
			}
		})
	}
	for _, address := range []string{"127.0.0.1", "10.1.2.3", "::1"} {
		t.Run(address, func(t *testing.T) {
			ip := net.ParseIP(address)
			if ip == nil {
				t.Fatalf("ParseIP(%q) = nil", address)
			}
			if !policy.allowsIP(ip) {
				t.Fatalf("allowsIP(%s) = false, want true with AllowPrivateIPs", address)
			}
		})
	}
}

func TestWebhookEgressChecksResolvedAddressesAtDialTime(t *testing.T) {
	// The hostname passes structural validation, but the DNS answer points at
	// the metadata service. Rebinding after submission is blocked the same way.
	resolver := func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{{IP: net.ParseIP("169.254.169.254")}}, nil
	}
	handler := NewWebhookDeliverHandler(WebhookEgressPolicy{})
	handler.resolver = resolver

	payload := `{"url":"https://webhook.example.com/hook"}`
	if err := handler.ValidatePayload([]byte(payload)); err != nil {
		t.Fatalf("ValidatePayload() error = %v, want structural validation to pass", err)
	}

	// The dialer reports why the address was refused; Execute sanitizes that
	// detail and classifies the failure as retryable.
	dial := webhookDialContext(WebhookEgressPolicy{}, resolver)
	_, err := dial(context.Background(), "tcp", "webhook.example.com:443")
	if err == nil || !strings.Contains(err.Error(), "not permitted") {
		t.Fatalf("dial error = %v, want resolved address rejected", err)
	}
	_, err = handler.Execute(context.Background(), testJob("webhook.deliver", payload))
	if err == nil || !strings.Contains(err.Error(), "request failed") {
		t.Fatalf("Execute() error = %v, want sanitized request failure", err)
	}
	if class := retry.Classify(err); class != retry.ClassTransient {
		t.Errorf("classification = %v, want transient", class)
	}
}

func TestWebhookEgressHostAllowList(t *testing.T) {
	tests := []struct {
		name    string
		hosts   []string
		url     string
		wantErr bool
	}{
		{"exact host", []string{"hooks.example.com"}, "https://hooks.example.com/hook", false},
		{"case insensitive", []string{"Hooks.Example.Com"}, "https://hooks.example.com/hook", false},
		{"trailing dot", []string{"hooks.example.com."}, "https://hooks.example.com/hook", false},
		{"subdomain", []string{".example.com"}, "https://a.b.example.com/hook", false},
		{"parent not implied", []string{".example.com"}, "https://example.com/hook", true},
		{"unlisted host", []string{"hooks.example.com"}, "https://evil.example.org/hook", true},
		{"suffix confusion", []string{".example.com"}, "https://example.com.evil.org/hook", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			handler := NewWebhookDeliverHandler(WebhookEgressPolicy{AllowedHosts: tt.hosts})
			err := handler.ValidatePayload([]byte(`{"url":"` + tt.url + `"}`))
			if tt.wantErr && (err == nil || !strings.Contains(err.Error(), "not allowed")) {
				t.Fatalf("ValidatePayload() error = %v, want host not allowed", err)
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ValidatePayload() error = %v, want nil", err)
			}
		})
	}
}

func TestWebhookEgressRejectsCredentialsAndHeaders(t *testing.T) {
	handler := NewWebhookDeliverHandler(WebhookEgressPolicy{})
	tests := []struct {
		name    string
		payload string
		wantErr string
	}{
		{"url credentials", `{"url":"https://user:pass@example.com/hook"}`, "credentials"},
		{"authorization header", `{"url":"https://example.com/hook","headers":{"Authorization":"Bearer secret"}}`, "not allowed"},
		{"routing header", `{"url":"https://example.com/hook","headers":{"Host":"internal"}}`, "not allowed"},
		{"content type header", `{"url":"https://example.com/hook","headers":{"Content-Type":"text/plain"}}`, "not allowed"},
		{"unknown header", `{"url":"https://example.com/hook","headers":{"X-Test":"yes"}}`, "not allowed"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := handler.ValidatePayload([]byte(tt.payload))
			if err == nil || !strings.Contains(err.Error(), tt.wantErr) {
				t.Fatalf("ValidatePayload() error = %v, want containing %q", err, tt.wantErr)
			}
		})
	}

	err := handler.ValidatePayload([]byte(`{"url":"https://example.com/hook","headers":{"X-Idempotency-Key":"abc"}}`))
	if err != nil {
		t.Fatalf("ValidatePayload() error = %v, want allowed header accepted", err)
	}
}

func TestWebhookEgressDoesNotFollowRedirects(t *testing.T) {
	var redirected atomic.Bool
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirected.Store(true)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	source := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, target.URL, http.StatusFound)
	}))
	defer source.Close()

	handler := localWebhookHandler()
	_, err := handler.Execute(context.Background(), testJob("webhook.deliver", `{"url":"`+source.URL+`"}`))
	if err == nil || !strings.Contains(err.Error(), "HTTP 302") {
		t.Fatalf("Execute() error = %v, want unfollowed HTTP 302", err)
	}
	if class := retry.Classify(err); class != retry.ClassPermanent {
		t.Errorf("classification = %v, want permanent", class)
	}
	if redirected.Load() {
		t.Error("redirect target was contacted")
	}
}

func TestWebhookEgressAllowsOnlyValidatedDNSAnswers(t *testing.T) {
	// A resolver answer containing one permitted and one blocked IP must not
	// silently fall through to the blocked address after a failed dial.
	resolver := func(context.Context, string) ([]net.IPAddr, error) {
		return []net.IPAddr{
			{IP: net.ParseIP("127.0.0.1")},
			{IP: net.ParseIP("169.254.169.254")},
		}, nil
	}
	dial := webhookDialContext(WebhookEgressPolicy{}, resolver)
	_, err := dial(context.Background(), "tcp", "webhook.example.com:443")
	if err == nil || !strings.Contains(err.Error(), "not permitted") {
		t.Fatalf("dial error = %v, want blocked address reported", err)
	}
}

func TestWebhookClientDisablesProxyAndRedirects(t *testing.T) {
	client := newWebhookHTTPClient(WebhookEgressPolicy{}, nil, nil)
	transport, ok := client.Transport.(*http.Transport)
	if !ok {
		t.Fatalf("transport = %T, want *http.Transport", client.Transport)
	}
	if transport.Proxy != nil {
		t.Error("proxy is enabled; it would bypass destination validation")
	}
	if transport.DialContext == nil {
		t.Error("validated dialer is not installed")
	}
	if client.CheckRedirect == nil {
		t.Fatal("redirect policy is not installed")
	}
	req, err := http.NewRequest(http.MethodGet, "https://example.com/hook", nil)
	if err != nil {
		t.Fatalf("NewRequest() error = %v", err)
	}
	if err := client.CheckRedirect(req, nil); !errors.Is(err, http.ErrUseLastResponse) {
		t.Errorf("CheckRedirect() error = %v, want ErrUseLastResponse", err)
	}
}

type stubRoundTripper struct{}

func (stubRoundTripper) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("custom transports are not permitted")
}

func TestWebhookClientReplacesCustomRoundTripper(t *testing.T) {
	client := newWebhookHTTPClient(WebhookEgressPolicy{}, &http.Client{Transport: stubRoundTripper{}}, nil)
	if _, ok := client.Transport.(*http.Transport); !ok {
		t.Errorf("transport = %T, want a validated *http.Transport", client.Transport)
	}
}

func TestWebhookDialContextRejectsInvalidTargets(t *testing.T) {
	t.Run("missing port", func(t *testing.T) {
		dial := webhookDialContext(WebhookEgressPolicy{}, nil)
		_, err := dial(context.Background(), "tcp", "example.com")
		if err == nil || !strings.Contains(err.Error(), "invalid destination") {
			t.Fatalf("dial error = %v, want invalid destination", err)
		}
	})
	t.Run("host outside allow list", func(t *testing.T) {
		policy := WebhookEgressPolicy{AllowedHosts: []string{"hooks.example.com"}}
		dial := webhookDialContext(policy, nil)
		_, err := dial(context.Background(), "tcp", "evil.example.org:443")
		if err == nil || !strings.Contains(err.Error(), "not allowed") {
			t.Fatalf("dial error = %v, want host not allowed", err)
		}
	})
	t.Run("no resolver addresses", func(t *testing.T) {
		dial := webhookDialContext(WebhookEgressPolicy{}, func(context.Context, string) ([]net.IPAddr, error) {
			return nil, nil
		})
		_, err := dial(context.Background(), "tcp", "example.com:443")
		if err == nil || !strings.Contains(err.Error(), "no addresses") {
			t.Fatalf("dial error = %v, want no addresses", err)
		}
	})
}

func TestWebhookEgressResolverFailureIsTransient(t *testing.T) {
	handler := NewWebhookDeliverHandler(WebhookEgressPolicy{})
	handler.resolver = func(context.Context, string) ([]net.IPAddr, error) {
		return nil, errors.New("dns unavailable")
	}
	_, err := handler.Execute(context.Background(), testJob("webhook.deliver", `{"url":"https://webhook.example.com/hook"}`))
	if err == nil || !strings.Contains(err.Error(), "request failed") {
		t.Fatalf("Execute() error = %v, want sanitized request failure", err)
	}
	if class := retry.Classify(err); class != retry.ClassTransient {
		t.Errorf("classification = %v, want transient", class)
	}
}
