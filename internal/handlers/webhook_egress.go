package handlers

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"net/url"
	"strings"
)

// WebhookEgressPolicy controls where webhook.deliver may connect. The zero
// value is safe for production: any public HTTP(S) host is allowed, but
// loopback, private, link-local, multicast and otherwise non-global addresses
// are blocked. DNS is resolved again for every new connection and every
// resolved address is checked, which closes the common DNS-rebinding path.
type WebhookEgressPolicy struct {
	// AllowedHosts, when non-empty, restricts destinations to these exact
	// hostnames. An entry beginning with "." permits that domain and all its
	// subdomains (for example ".example.com"). Matching is case-insensitive.
	AllowedHosts []string

	// AllowPrivateIPs permits loopback, private, link-local and other
	// non-global addresses. It exists for local development and tests only;
	// enabling it in a production worker reintroduces SSRF exposure.
	AllowPrivateIPs bool
}

// ipResolver is the DNS lookup seam used by the dialer. Tests replace it to
// simulate rebinding without depending on external DNS.
type ipResolver func(ctx context.Context, host string) ([]net.IPAddr, error)

func defaultIPResolver(ctx context.Context, host string) ([]net.IPAddr, error) {
	return net.DefaultResolver.LookupIPAddr(ctx, host)
}

// allowsHost applies the optional hostname allow-list.
func (p WebhookEgressPolicy) allowsHost(host string) bool {
	host = strings.ToLower(strings.TrimSuffix(strings.TrimSpace(host), "."))
	if host == "" {
		return false
	}
	if len(p.AllowedHosts) == 0 {
		return true
	}
	for _, raw := range p.AllowedHosts {
		rule := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(raw), "."))
		if rule == "" {
			continue
		}
		if strings.HasPrefix(rule, ".") {
			if strings.HasSuffix(host, rule) {
				return true
			}
		} else if host == rule {
			return true
		}
	}
	return false
}

// blockedWebhookPrefixes covers ranges that net.IP still classifies as global
// unicast but that a tenant-controlled webhook must never reach.
var blockedWebhookPrefixes = []netip.Prefix{
	netip.MustParsePrefix("100.64.0.0/10"),   // carrier-grade NAT
	netip.MustParsePrefix("192.0.0.0/24"),    // IETF protocol assignments
	netip.MustParsePrefix("192.0.2.0/24"),    // documentation TEST-NET-1
	netip.MustParsePrefix("198.18.0.0/15"),   // benchmarking
	netip.MustParsePrefix("198.51.100.0/24"), // documentation TEST-NET-2
	netip.MustParsePrefix("203.0.113.0/24"),  // documentation TEST-NET-3
	netip.MustParsePrefix("240.0.0.0/4"),     // reserved
	netip.MustParsePrefix("2001:db8::/32"),   // documentation
	netip.MustParsePrefix("2001:10::/28"),    // ORCHIDv2
}

// allowsIP rejects every address that is not globally routable unless the
// policy explicitly opts in to private connectivity.
func (p WebhookEgressPolicy) allowsIP(ip net.IP) bool {
	if ip == nil {
		return false
	}
	if v4 := ip.To4(); v4 != nil {
		ip = v4
	}
	if p.AllowPrivateIPs {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() || ip.IsInterfaceLocalMulticast() ||
		ip.IsMulticast() || ip.IsUnspecified() || !ip.IsGlobalUnicast() {
		return false
	}
	if addr, ok := netip.AddrFromSlice(ip); ok {
		addr = addr.Unmap()
		for _, prefix := range blockedWebhookPrefixes {
			if prefix.Contains(addr) {
				return false
			}
		}
	}
	return true
}

// validateWebhookTarget performs the cheap, structural part of egress
// validation. Authoritative IP checks happen at dial time in
// webhookDialContext, because DNS answers can change after submission.
func validateWebhookTarget(policy WebhookEgressPolicy, target *url.URL) error {
	if target == nil || strings.TrimSpace(target.Host) == "" {
		return errors.New("webhook.deliver requires a valid url")
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return errors.New("webhook.deliver requires an http or https url")
	}
	if target.User != nil {
		return errors.New("webhook.deliver url must not contain credentials")
	}

	host := target.Hostname()
	if !policy.allowsHost(host) {
		return fmt.Errorf("webhook.deliver destination host %q is not allowed", host)
	}
	// Reject literal private addresses at submission so callers get immediate
	// feedback. Hostnames are re-resolved and checked again at execution.
	if ip := net.ParseIP(host); ip != nil && !policy.allowsIP(ip) {
		return fmt.Errorf("webhook.deliver destination address %s is not permitted", ip)
	}
	return nil
}

// webhookDialContext returns a dialer that resolves the destination itself,
// validates every answer, and dials only a permitted address. Dialing the
// resolved IP while leaving the URL host intact preserves the Host header and
// TLS SNI, and prevents a second lookup (and therefore rebinding) inside net/http.
func webhookDialContext(policy WebhookEgressPolicy, resolve ipResolver) func(context.Context, string, string) (net.Conn, error) {
	if resolve == nil {
		resolve = defaultIPResolver
	}
	dialer := &net.Dialer{}

	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("webhook.deliver invalid destination: %w", err)
		}
		if !policy.allowsHost(host) {
			return nil, fmt.Errorf("webhook.deliver destination host %q is not allowed", host)
		}

		var candidates []net.IPAddr
		if ip := net.ParseIP(host); ip != nil {
			candidates = []net.IPAddr{{IP: ip}}
		} else {
			candidates, err = resolve(ctx, host)
			if err != nil {
				return nil, fmt.Errorf("webhook.deliver destination resolution failed: %w", err)
			}
		}
		if len(candidates) == 0 {
			return nil, errors.New("webhook.deliver destination has no addresses")
		}

		var lastErr error
		for _, candidate := range candidates {
			if !policy.allowsIP(candidate.IP) {
				lastErr = fmt.Errorf("webhook.deliver destination address %s is not permitted", candidate.IP)
				continue
			}
			conn, err := dialer.DialContext(ctx, network, net.JoinHostPort(candidate.IP.String(), port))
			if err == nil {
				return conn, nil
			}
			lastErr = err
		}
		if lastErr == nil {
			lastErr = errors.New("webhook.deliver destination has no permitted address")
		}
		return nil, lastErr
	}
}

// newWebhookHTTPClient builds a client with the egress dialer installed and
// redirects disabled. A 3xx response is returned to the handler, which treats
// it as a permanent failure instead of following it into another destination.
func newWebhookHTTPClient(policy WebhookEgressPolicy, base *http.Client, resolve ipResolver) *http.Client {
	client := &http.Client{}
	if base != nil {
		cloned := *base
		client = &cloned
	}
	// A caller may shorten or lengthen the timeout, but a zero value must not
	// silently remove the bounded webhook request window.
	if client.Timeout <= 0 {
		client.Timeout = webhookTimeout
	}

	var transport *http.Transport
	switch current := client.Transport.(type) {
	case *http.Transport:
		transport = current.Clone()
	case nil:
		transport = http.DefaultTransport.(*http.Transport).Clone()
	default:
		// A custom RoundTripper cannot enforce IP-level egress controls, so
		// fall back to a validated transport rather than bypassing the policy.
		transport = http.DefaultTransport.(*http.Transport).Clone()
	}
	transport.DialContext = webhookDialContext(policy, resolve)
	// A proxy would forward the request from its own network, bypassing the
	// destination checks performed by our dialer.
	transport.Proxy = nil
	client.Transport = transport
	client.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return client
}
