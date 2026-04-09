package p2p

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfig_IPFS_Mainnet(t *testing.T) {
	cfg, err := GetBootstrapConfig(ProjectIPFS, NetworkMainnet)
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
	assert.Len(t, cfg.ProtocolIDs, 2)
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
