package executor

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"time"
)

// newSSRFSafeTransport returns an http.Transport that blocks connections to
// private/reserved IP ranges, preventing Server-Side Request Forgery (SSRF).
func newSSRFSafeTransport() *http.Transport {
	return &http.Transport{
		MaxIdleConns:        50,
		MaxIdleConnsPerHost: 10,
		MaxConnsPerHost:     20,
		IdleConnTimeout:     90 * time.Second,
		ForceAttemptHTTP2:   true,
		DialContext:         ssrfSafeDialer(),
	}
}

// ssrfSafeDialer returns a DialContext function that resolves the hostname
// and rejects connections to private/reserved IP addresses.
func ssrfSafeDialer() func(ctx context.Context, network, addr string) (net.Conn, error) {
	dialer := &net.Dialer{Timeout: 10 * time.Second}
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, fmt.Errorf("invalid address: %w", err)
		}

		ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
		if err != nil {
			return nil, fmt.Errorf("DNS resolution failed: %w", err)
		}

		for _, ip := range ips {
			if isPrivateIP(ip.IP) {
				return nil, fmt.Errorf("SSRF protection: connections to private/reserved IP %s are blocked", ip.IP)
			}
		}

		// Connect to the first allowed IP
		return dialer.DialContext(ctx, network, net.JoinHostPort(ips[0].IP.String(), port))
	}
}

// isPrivateIP returns true if the IP is in a private, loopback, link-local,
// or other reserved range that should not be reachable from user-controlled requests.
func isPrivateIP(ip net.IP) bool {
	// Loopback (127.0.0.0/8, ::1)
	if ip.IsLoopback() {
		return true
	}
	// Link-local unicast (169.254.0.0/16, fe80::/10) — includes cloud metadata
	if ip.IsLinkLocalUnicast() {
		return true
	}
	// Link-local multicast
	if ip.IsLinkLocalMulticast() {
		return true
	}
	// Unspecified (0.0.0.0, ::)
	if ip.IsUnspecified() {
		return true
	}

	// Private ranges (RFC 1918 + RFC 4193)
	privateRanges := []string{
		"10.0.0.0/8",
		"172.16.0.0/12",
		"192.168.0.0/16",
		"fc00::/7",
	}
	for _, cidr := range privateRanges {
		_, network, err := net.ParseCIDR(cidr)
		if err != nil {
			continue
		}
		if network.Contains(ip) {
			return true
		}
	}

	return false
}
