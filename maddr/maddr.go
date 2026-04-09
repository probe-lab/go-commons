// Package maddr provides common multiaddress transformation and filtering
// utilities. Adapted from github.com/dennis-tra/nebula utils/utils.go.
package maddr

import (
	"fmt"
	"net"
	"strings"

	ma "github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"
)

// ToStrings converts a slice of multiaddresses to their string representations.
func ToStrings(maddrs []ma.Multiaddr) []string {
	out := make([]string, len(maddrs))
	for i, m := range maddrs {
		out[i] = m.String()
	}
	return out
}

// FromStrings parses a slice of multiaddress strings. Returns an error if any
// string is not a valid multiaddress.
func FromStrings(addrs []string) ([]ma.Multiaddr, error) {
	out := make([]ma.Multiaddr, len(addrs))
	for i, s := range addrs {
		m, err := ma.NewMultiaddr(s)
		if err != nil {
			return nil, fmt.Errorf("parse multiaddr %q: %w", s, err)
		}
		out[i] = m
	}
	return out, nil
}

// MustFromStrings parses a slice of multiaddress strings.
// It panics if any string is not a valid multiaddress.
func MustFromStrings(addrs []string) []ma.Multiaddr {
	out := make([]ma.Multiaddr, len(addrs))
	for i, s := range addrs {
		m, err := ma.NewMultiaddr(s)
		if err != nil {
			panic(fmt.Errorf("parse multiaddr %q: %w", s, err))
		}
		out[i] = m
	}
	return out
}

// FilterPrivate removes private/loopback multiaddresses from the slice.
func FilterPrivate(maddrs []ma.Multiaddr) []ma.Multiaddr {
	var out []ma.Multiaddr
	for _, m := range maddrs {
		if manet.IsPrivateAddr(m) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// FilterPublic removes public multiaddresses from the slice, keeping only
// private/loopback addresses.
func FilterPublic(maddrs []ma.Multiaddr) []ma.Multiaddr {
	var out []ma.Multiaddr
	for _, m := range maddrs {
		if manet.IsPublicAddr(m) {
			continue
		}
		out = append(out, m)
	}
	return out
}

// Merge combines two multiaddress slices, deduplicating by byte representation.
func Merge(a, b []ma.Multiaddr) []ma.Multiaddr {
	seen := make(map[string]struct{}, len(a)+len(b))
	var out []ma.Multiaddr

	for _, m := range a {
		key := string(m.Bytes())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, m)
	}

	for _, m := range b {
		key := string(m.Bytes())
		if _, ok := seen[key]; ok {
			continue
		}
		seen[key] = struct{}{}
		out = append(out, m)
	}

	return out
}

// ExtractIP returns the first IP address found in the multiaddress.
// Returns nil if no IP component is present.
func ExtractIP(m ma.Multiaddr) net.IP {
	if v, err := m.ValueForProtocol(ma.P_IP4); err == nil {
		return net.ParseIP(v)
	}
	if v, err := m.ValueForProtocol(ma.P_IP6); err == nil {
		return net.ParseIP(v)
	}
	return nil
}

// ExtractIPFromString extracts an IP address from a multiaddress string.
// Returns nil if the string is not a valid multiaddress or has no IP component.
func ExtractIPFromString(s string) net.IP {
	m, err := ma.NewMultiaddr(s)
	if err != nil {
		return nil
	}
	return ExtractIP(m)
}

// EllipsizeCerthash shortens certhash components in a multiaddress string for
// display purposes. Components after "/certhash/" are truncated to maxLen.
func EllipsizeCerthash(input string, maxLen int) string {
	parts := strings.Split(input, "/")
	for i := 0; i < len(parts); i++ {
		if parts[i] == "certhash" && i+1 < len(parts) {
			parts[i+1] = ellipsize(parts[i+1], maxLen)
		}
	}
	return strings.Join(parts, "/")
}

func ellipsize(s string, maxLen int) string {
	if len(s) <= maxLen {
		return s
	}
	half := (maxLen - 2) / 2
	if half < 1 {
		half = 1
	}
	return s[:half] + ".." + s[len(s)-half:]
}
