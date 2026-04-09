package maddr

import (
	"testing"

	ma "github.com/multiformats/go-multiaddr"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func mustMA(t *testing.T, s string) ma.Multiaddr {
	t.Helper()
	m, err := ma.NewMultiaddr(s)
	require.NoError(t, err)
	return m
}

func TestToStrings(t *testing.T) {
	maddrs := []ma.Multiaddr{
		mustMA(t, "/ip4/1.2.3.4/tcp/80"),
		mustMA(t, "/ip6/::1/udp/443"),
	}
	strs := ToStrings(maddrs)
	assert.Equal(t, []string{"/ip4/1.2.3.4/tcp/80", "/ip6/::1/udp/443"}, strs)
}

func TestFromStrings(t *testing.T) {
	strs := []string{"/ip4/1.2.3.4/tcp/80", "/ip6/::1/udp/443"}
	maddrs, err := FromStrings(strs)
	require.NoError(t, err)
	assert.Len(t, maddrs, 2)
	assert.Equal(t, "/ip4/1.2.3.4/tcp/80", maddrs[0].String())
}

func TestFromStrings_Invalid(t *testing.T) {
	_, err := FromStrings([]string{"not-a-multiaddr"})
	assert.Error(t, err)
}

func TestFilterPrivate(t *testing.T) {
	maddrs := []ma.Multiaddr{
		mustMA(t, "/ip4/1.2.3.4/tcp/80"),
		mustMA(t, "/ip4/127.0.0.1/tcp/80"),
		mustMA(t, "/ip4/192.168.1.1/tcp/80"),
	}
	public := FilterPrivate(maddrs)
	assert.Len(t, public, 1)
	assert.Equal(t, "/ip4/1.2.3.4/tcp/80", public[0].String())
}

func TestFilterPublic(t *testing.T) {
	maddrs := []ma.Multiaddr{
		mustMA(t, "/ip4/1.2.3.4/tcp/80"),
		mustMA(t, "/ip4/127.0.0.1/tcp/80"),
		mustMA(t, "/ip4/192.168.1.1/tcp/80"),
	}
	private := FilterPublic(maddrs)
	assert.Len(t, private, 2)
}

func TestMerge(t *testing.T) {
	a := []ma.Multiaddr{mustMA(t, "/ip4/1.2.3.4/tcp/80")}
	b := []ma.Multiaddr{
		mustMA(t, "/ip4/1.2.3.4/tcp/80"), // duplicate
		mustMA(t, "/ip4/5.6.7.8/tcp/80"),
	}
	merged := Merge(a, b)
	assert.Len(t, merged, 2)
}

func TestExtractIP_IPv4(t *testing.T) {
	m := mustMA(t, "/ip4/1.2.3.4/tcp/80")
	ip := ExtractIP(m)
	require.NotNil(t, ip)
	assert.Equal(t, "1.2.3.4", ip.String())
}

func TestExtractIP_IPv6(t *testing.T) {
	m := mustMA(t, "/ip6/::1/tcp/80")
	ip := ExtractIP(m)
	require.NotNil(t, ip)
	assert.Equal(t, "::1", ip.String())
}

func TestExtractIP_NoIP(t *testing.T) {
	m := mustMA(t, "/dns4/example.com/tcp/80")
	ip := ExtractIP(m)
	assert.Nil(t, ip)
}

func TestExtractIPFromString(t *testing.T) {
	ip := ExtractIPFromString("/ip4/10.0.0.1/tcp/80")
	require.NotNil(t, ip)
	assert.Equal(t, "10.0.0.1", ip.String())
}

func TestExtractIPFromString_Invalid(t *testing.T) {
	ip := ExtractIPFromString("not-a-multiaddr")
	assert.Nil(t, ip)
}

func TestEllipsizeCerthash(t *testing.T) {
	input := "/ip4/1.2.3.4/udp/443/quic-v1/webtransport/certhash/uEiAWlgd8EqbNhYLv86OdRvXHMosaUWFFDbhgGZgCkcmKnQ/p2p/12D3KooW"
	result := EllipsizeCerthash(input, 10)
	assert.Contains(t, result, "uEiA..mKnQ")
	assert.Contains(t, result, "/p2p/12D3KooW")
}
