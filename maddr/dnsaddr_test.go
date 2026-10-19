package maddr

import (
	"context"
	"errors"
	"net"
	"testing"

	madns "github.com/multiformats/go-multiaddr-dns"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	testPeerA = "12D3KooWKKkCZbcigsWTEu1cgNetNbZJqeNtysRtFpq7DTqw3eqH"
	testPeerB = "12D3KooWGnkd9GQKo3apkShQDaq1d6cKJJmsVe6KiQkacUk1T8oZ"
)

// fakeDNS answers TXT lookups from a map, or fails them all with err.
type fakeDNS struct {
	txt     map[string][]string
	err     error
	lookups int
}

func (f *fakeDNS) LookupIPAddr(context.Context, string) ([]net.IPAddr, error) {
	return nil, errors.New("unexpected IP lookup")
}

func (f *fakeDNS) LookupTXT(_ context.Context, name string) ([]string, error) {
	f.lookups++
	if f.err != nil {
		return nil, f.err
	}
	return f.txt[name], nil
}

func newTestResolver(t *testing.T, f *fakeDNS) *madns.Resolver {
	t.Helper()
	r, err := madns.NewResolver(madns.WithDefaultResolver(f))
	require.NoError(t, err)
	return r
}

func TestIsDNSAddr(t *testing.T) {
	assert.True(t, IsDNSAddr(mustMA(t, "/dnsaddr/example.com")))
	assert.True(t, IsDNSAddr(mustMA(t, "/dnsaddr/example.com/p2p/"+testPeerA)))
	assert.False(t, IsDNSAddr(mustMA(t, "/dns4/example.com/tcp/4001")))
	assert.False(t, IsDNSAddr(mustMA(t, "/ip4/1.2.3.4/tcp/4001")))
}

func TestResolveDNSAddr(t *testing.T) {
	f := &fakeDNS{txt: map[string][]string{
		"_dnsaddr.example.com": {
			"dnsaddr=/ip4/1.2.3.4/tcp/4001/p2p/" + testPeerA,
			"dnsaddr=/ip4/1.2.3.4/udp/4001/quic-v1/p2p/" + testPeerA,
			"dnsaddr=/dnsaddr/sub.example.com/p2p/" + testPeerB,
			"dnsaddr=/ip4/5.6.7.8/tcp/4001", // no /p2p/
			"dnsaddr=/not/a/multiaddr",
			"v=spf1 -all",
		},
	}}

	got, err := ResolveDNSAddr(context.Background(), newTestResolver(t, f), mustMA(t, "/dnsaddr/example.com"))
	require.NoError(t, err)
	assert.Equal(t, []string{
		"/ip4/1.2.3.4/tcp/4001/p2p/" + testPeerA,
		"/ip4/1.2.3.4/udp/4001/quic-v1/p2p/" + testPeerA,
		"/dnsaddr/sub.example.com/p2p/" + testPeerB,
	}, ToStrings(got))
	assert.Equal(t, 1, f.lookups)
}

func TestResolveDNSAddr_NotDNSAddr(t *testing.T) {
	for _, s := range []string{
		"/ip4/1.2.3.4/tcp/4001",
		"/dns4/example.com/tcp/4001",
		"/ip4/1.2.3.4/tcp/4001/p2p/" + testPeerA + "/p2p-circuit",
	} {
		f := &fakeDNS{}
		_, err := ResolveDNSAddr(context.Background(), newTestResolver(t, f), mustMA(t, s))
		assert.ErrorContains(t, err, "not a /dnsaddr address", s)
		assert.Zero(t, f.lookups, s)
	}
}

func TestResolveDNSAddr_LookupFails(t *testing.T) {
	lookupErr := &net.DNSError{Err: "no such host", Name: "_dnsaddr.example.com", IsNotFound: true}
	f := &fakeDNS{err: lookupErr}

	_, err := ResolveDNSAddr(context.Background(), newTestResolver(t, f), mustMA(t, "/dnsaddr/example.com"))
	assert.ErrorIs(t, err, lookupErr)
	assert.ErrorContains(t, err, "/dnsaddr/example.com")
}

func TestResolveDNSAddr_NoPeers(t *testing.T) {
	tests := map[string][]string{
		"no records":         nil,
		"no dnsaddr records": {"v=spf1 -all"},
		"no /p2p/":           {"dnsaddr=/ip4/5.6.7.8/tcp/4001"},
		// A record that points to its own name is not followed.
		"loop": {"dnsaddr=/dnsaddr/example.com"},
	}

	for name, records := range tests {
		t.Run(name, func(t *testing.T) {
			f := &fakeDNS{txt: map[string][]string{"_dnsaddr.example.com": records}}

			_, err := ResolveDNSAddr(context.Background(), newTestResolver(t, f), mustMA(t, "/dnsaddr/example.com"))
			assert.ErrorContains(t, err, "no listed address ends in /p2p/")
			assert.Equal(t, 1, f.lookups)
		})
	}
}

func TestResolveDNSAddr_Suffix(t *testing.T) {
	// madns keeps only the records that end in the components after the
	// /dnsaddr name.
	f := &fakeDNS{txt: map[string][]string{
		"_dnsaddr.example.com": {
			"dnsaddr=/ip4/1.2.3.4/tcp/4001/p2p/" + testPeerA,
			"dnsaddr=/ip4/1.2.3.4/tcp/4001/p2p/" + testPeerB,
		},
	}}

	got, err := ResolveDNSAddr(context.Background(), newTestResolver(t, f), mustMA(t, "/dnsaddr/example.com/p2p/"+testPeerB))
	require.NoError(t, err)
	assert.Equal(t, []string{"/ip4/1.2.3.4/tcp/4001/p2p/" + testPeerB}, ToStrings(got))

	_, err = ResolveDNSAddr(context.Background(), newTestResolver(t, f), mustMA(t, "/dnsaddr/example.com/tcp/4001"))
	assert.ErrorContains(t, err, "no listed address ends in /p2p/")
}
