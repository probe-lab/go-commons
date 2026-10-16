package p2p

import (
	"net/netip"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_IPFS_Amino(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectIPFS, NetworkAmino)
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.Bootstrappers)
	assert.NotEmpty(t, cfg.ProtocolIDs)
	assert.Equal(t, "/ipfs/kad/1.0.0", cfg.ProtocolIDs[0])
}

func TestConfig_Celestia_Mainnet(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectCelestia, NetworkMainnet)
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.Bootstrappers)
	assert.Contains(t, cfg.ProtocolIDs[0], "celestia")
}

func TestConfig_Filecoin_Calibnet(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectFilecoin, NetworkCalibnet)
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.Bootstrappers)
	assert.Contains(t, cfg.ProtocolIDs[0], "calibrationnet")
}

func TestConfig_Polkadot_Mainnet(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectPolkadot, NetworkMainnet)
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.Bootstrappers)
	assert.Equal(t, []string{"/91b171bb158e2d3848fa23a9f1c25182fb8e20313b2c1eb49219da7a70ce90c3/kad"}, cfg.ProtocolIDs)
}

func TestConfig_Agntcy_Mainnet(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectAgntcy, NetworkMainnet)
	require.NoError(t, err)
	assert.NotEmpty(t, cfg.Bootstrappers)
	assert.Equal(t, []string{"dir/kad/1.0.0"}, cfg.ProtocolIDs)
}

func TestConfig_Ethereum(t *testing.T) {
	for _, network := range []Network{NetworkDiscv5, NetworkDiscv4} {
		cfg, err := GetBootstrapConfig(ProjectEthereum, network)
		require.NoError(t, err)
		assert.NotEmpty(t, cfg.Bootstrappers)
		assert.Equal(t, []string{string(network)}, cfg.ProtocolIDs)

		// geth lists both ENRs and enode URLs; crawlers must parse both.
		for _, b := range cfg.Bootstrappers {
			assert.True(t, strings.HasPrefix(b, "enr:") || strings.HasPrefix(b, "enode://"), b)
		}
	}
}

func TestConfig_Unknown_Project(t *testing.T) {
	_, err := GetBootstrapConfig("nonexistent", NetworkMainnet)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown project")
}

func TestConfig_Unknown_Network(t *testing.T) {
	_, err := GetBootstrapConfig(ProjectIPFS, "nonexistent")
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unknown network")
}

func TestConfig_AlwaysTwoParams(t *testing.T) {
	// Verify that every config entry requires both project and network
	for _, cfg := range SupportedBootstrapConfigs() {
		assert.NotEmpty(t, string(cfg.Project), "project must not be empty")
		assert.NotEmpty(t, string(cfg.Network), "network must not be empty")
		assert.NotEmpty(t, cfg.Bootstrappers, "bootstrap peers must not be empty for %s/%s", cfg.Project, cfg.Network)
		assert.NotEmpty(t, cfg.ProtocolIDs, "protocolIDs must not be empty for %s/%s", cfg.Project, cfg.Network)
	}
}

func TestSupportedBootstrapConfigs(t *testing.T) {
	cfgs := SupportedBootstrapConfigs()
	assert.NotEmpty(t, cfgs)

	// Check that we have at least the major projects
	projects := make(map[Project]bool)
	for _, cfg := range cfgs {
		projects[cfg.Project] = true
	}
	assert.True(t, projects[ProjectIPFS])
	assert.True(t, projects[ProjectFilecoin])
	assert.True(t, projects[ProjectCelestia])
	assert.True(t, projects[ProjectPolkadot])
	assert.True(t, projects[ProjectEthereum])
}

func TestConfig_BitcoinFamily(t *testing.T) {
	for project, magic := range map[Project]string{
		ProjectBitcoin:     "0xd9b4bef9",
		ProjectBitcoinCash: "0xe8f3e1e3",
		ProjectLitecoin:    "0xdbb6c0fb",
		ProjectDogecoin:    "0xc0c0c0c0",
		ProjectZCash:       "0x6427e924",
		ProjectDash:        "0xbd6b0cbf",
		ProjectDigiByte:    "0xdab6c3fa",
		ProjectRavencoin:   "0x4e564152",
		ProjectFlux:        "0x6427e924",
	} {
		cfg, err := GetBootstrapConfig(project, NetworkMainnet)
		require.NoError(t, err)
		assert.Equal(t, []string{magic}, cfg.ProtocolIDs, project)
		assert.NotEmpty(t, cfg.Bootstrappers, project)
	}
}

func TestConfig_HyperDHT_Mainnet(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectHyperDHT, NetworkMainnet)
	require.NoError(t, err)
	assert.Equal(t, []string{"hyperdht"}, cfg.ProtocolIDs)
	assert.Contains(t, cfg.Bootstrappers, "88.99.3.86@node1.hyperdht.org:49737")
}

func TestConfig_BitTorrent_Mainnet(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectBitTorrent, NetworkMainnet)
	require.NoError(t, err)
	assert.Equal(t, []string{"bep5"}, cfg.ProtocolIDs)
	assert.Contains(t, cfg.Bootstrappers, "router.bittorrent.com:6881")
}

func TestConfig_Tor_Mainnet(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectTor, NetworkMainnet)
	require.NoError(t, err)
	assert.Equal(t, []string{"tor"}, cfg.ProtocolIDs)
	assert.Len(t, cfg.Bootstrappers, 9)
	for _, b := range cfg.Bootstrappers {
		_, err := netip.ParseAddrPort(b)
		assert.NoError(t, err, b)
	}
}
