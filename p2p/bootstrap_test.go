package p2p

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBootstrapPeers_IPFS(t *testing.T) {
	peers := BootstrapPeers("ipfs")
	require.NotEmpty(t, peers)
	for _, p := range peers {
		assert.Contains(t, p, "/p2p/")
	}
}

func TestBootstrapPeers_Celestia(t *testing.T) {
	peers := BootstrapPeers("celestia")
	require.NotEmpty(t, peers)
}

func TestBootstrapPeers_Unknown(t *testing.T) {
	peers := BootstrapPeers("unknown-project")
	assert.Nil(t, peers)
}

func TestDHTProtocolID_IPFS(t *testing.T) {
	proto := DHTProtocolID("ipfs")
	assert.Equal(t, "/ipfs/kad/1.0.0", proto)
}

func TestDHTProtocolID_Celestia(t *testing.T) {
	proto := DHTProtocolID("celestia")
	assert.NotEmpty(t, proto)
}

func TestDHTProtocolID_Unknown(t *testing.T) {
	proto := DHTProtocolID("unknown-project")
	assert.Empty(t, proto)
}

func TestSupportedProjects(t *testing.T) {
	projects := SupportedProjects()
	assert.Contains(t, projects, "ipfs")
	assert.Contains(t, projects, "celestia")
}
