// Package p2p provides well-known peer-to-peer network configuration such as
// bootstrap peer addresses and Kademlia DHT protocol identifiers. These values
// are reused across multiple ProbeLab services.
package p2p

import "fmt"

// BootstrapConfig holds the bootstrap and protocol configuration for a specific
// project+network combination.
type BootstrapConfig struct {
	Project       Project
	Network       Network
	Bootstrappers []string
	ProtocolIDs   []string
}

// GetBootstrapConfig returns the network configuration for the given project+network pair.
// Returns an error if the combination is unknown.
func GetBootstrapConfig(project Project, network Network) (BootstrapConfig, error) {
	networks, ok := configs[project]
	if !ok {
		return BootstrapConfig{}, fmt.Errorf("unknown project: %s", project)
	}

	cfg, ok := networks[network]
	if !ok {
		return BootstrapConfig{}, fmt.Errorf("unknown network %s for project %s", network, project)
	}

	return *cfg, nil
}

// MustConfig returns the network configuration for the given project+network pair.
// Panics if the combination is unknown.
func MustBootstrapConfig(project Project, network Network) BootstrapConfig {
	cfg, err := GetBootstrapConfig(project, network)
	if err != nil {
		panic(err)
	}
	return cfg
}

// SupportedConfigs returns all registered (project, network) pairs.
func SupportedBootstrapConfigs() []BootstrapConfig {
	var out []BootstrapConfig
	for project, networks := range configs {
		for network, cfg := range networks {
			out = append(out, BootstrapConfig{
				Project:       project,
				Network:       network,
				Bootstrappers: cfg.Bootstrappers,
				ProtocolIDs:   cfg.ProtocolIDs,
			})
		}
	}

	return out
}

// configs is the registry of all known project+network configurations.
var configs = map[Project]map[Network]*BootstrapConfig{
	ProjectIPFS: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersIPFSAmino,
			ProtocolIDs:   []string{"/ipfs/kad/1.0.0"},
		},
		NetworkAmino: {
			Bootstrappers: BootstrapPeersIPFSAmino,
			ProtocolIDs:   []string{"/ipfs/kad/1.0.0"},
		},
	},
	ProjectFilecoin: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersFilecoinMainnet,
			ProtocolIDs:   []string{"/fil/kad/testnetnet/kad/1.0.0"},
		},
		NetworkCalibnet: {
			Bootstrappers: BootstrapPeersFilecoinCalibnet,
			ProtocolIDs:   []string{"/fil/kad/calibrationnet/kad/1.0.0"},
		},
	},
	ProjectCelestia: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersCelestia,
			ProtocolIDs:   []string{"/celestia/celestia/kad/1.0.0"},
		},
		NetworkArabica: {
			Bootstrappers: BootstrapPeersArabica,
			ProtocolIDs:   []string{"/celestia/arabica-10/kad/1.0.0"},
		},
		NetworkMocha: {
			Bootstrappers: BootstrapPeersMocha,
			ProtocolIDs:   []string{"/celestia/mocha-4/kad/1.0.0"},
		},
		NetworkBlockRace: {
			Bootstrappers: BootstrapPeersBlockspaceRace,
			ProtocolIDs:   []string{"/celestia/blockspacerace-0/kad/1.0.0"},
		},
	},
	ProjectPolkadot: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersPolkadot,
			ProtocolIDs:   []string{"/dot/kad", "/91b171bb158e2d3848fa23a9f1c25182fb8e20313b2c1eb49219da7a70ce90c3/kad"},
		},
		NetworkKusama: {
			Bootstrappers: BootstrapPeersKusama,
			ProtocolIDs:   []string{"/ksmcc3/kad"},
		},
		NetworkRococo: {
			Bootstrappers: BootstrapPeersRococo,
			ProtocolIDs:   []string{"/rococo/kad"},
		},
		NetworkWestend: {
			Bootstrappers: BootstrapPeersWestend,
			ProtocolIDs:   []string{"/wnd2/kad"},
		},
	},
	ProjectEthereum: {
		NetworkConsensus: {
			Bootstrappers: BootstrapPeersEthereumConsensus,
			ProtocolIDs:   []string{"discv5"},
		},
		NetworkExecution: {
			Bootstrappers: BootstrapPeersEthereumExecution,
			ProtocolIDs:   []string{"discv4"},
		},
		NetworkHolesky: {
			Bootstrappers: BootstrapPeersHolesky,
			ProtocolIDs:   []string{"discv5"},
		},
	},
	ProjectGnosis: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersGnosis,
			ProtocolIDs:   []string{"discv5"},
		},
	},
	ProjectPortal: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersPortalMainnet,
			ProtocolIDs:   []string{"discv5"},
		},
	},
	ProjectAvail: {
		NetworkMainnetFN: {
			Bootstrappers: BootstrapPeersAvailMainnetFullNode,
			ProtocolIDs:   []string{"/Avail/kad"},
		},
		NetworkMainnetLC: {
			Bootstrappers: BootstrapPeersAvailMainnetLightClient,
			ProtocolIDs:   []string{"/avail_kad/id/1.0.0-b91746"},
		},
		NetworkTuringFN: {
			Bootstrappers: BootstrapPeersAvailTuringFullNode,
			ProtocolIDs:   []string{"/Avail/kad"},
		},
		NetworkTuringLC: {
			Bootstrappers: BootstrapPeersAvailTuringLightClient,
			ProtocolIDs:   []string{"/avail_kad/id/1.0.0-6f0996"},
		},
	},
	ProjectPactus: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersPactusFullNode,
			ProtocolIDs:   []string{"/pactus/gossip/v1/kad/1.0.0"},
		},
	},
	ProjectBitcoin: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersBitcoin,
			ProtocolIDs:   []string{"bitcoin"},
		},
	},
	ProjectDria: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersDria,
			ProtocolIDs:   []string{"/dria/kad/0.2"},
		},
	},
	ProjectWaku: {
		NetworkStatus: {
			Bootstrappers: BootstrapPeersWakuStatus,
			ProtocolIDs:   []string{"d5waku"},
		},
		NetworkTWN: {
			Bootstrappers: BootstrapPeersWakuTWN,
			ProtocolIDs:   []string{"d5waku"},
		},
	},
	ProjectMonero: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersMoneroMainnet,
			ProtocolIDs:   []string{"\x12\x30\xf1\x71\x61\x04\x41\x61\x17\x31\x00\x82\x16\xa1\xa1\x10"},
		},
	},
	ProjectAlgorand: {
		NetworkMainnet: {
			Bootstrappers: BootstrapPeersAlgorandMainnet,
			ProtocolIDs:   []string{"/algorand/kad/mainnet/kad/1.0.0"},
		},
	},
}
