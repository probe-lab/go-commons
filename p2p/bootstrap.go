// Package p2p provides well-known peer-to-peer network constants such as
// bootstrap peer multiaddresses and Kademlia DHT protocol identifiers.
// These values are reused across multiple ProbeLab services.
package p2p

// BootstrapPeers returns the well-known bootstrap peer multiaddresses for
// the given project. Returns nil for unknown projects.
func BootstrapPeers(project string) []string {
	switch project {
	case "ipfs":
		return ipfsBootstrapPeers
	case "celestia":
		return celestiaBootstrapPeers
	default:
		return nil
	}
}

// DHTProtocolID returns the Kademlia DHT protocol ID string for the given
// project. Returns an empty string for unknown projects.
func DHTProtocolID(project string) string {
	switch project {
	case "ipfs":
		return "/ipfs/kad/1.0.0"
	case "celestia":
		// placeholder — replace with real Celestia protocol ID before going live
		return "/celestia/kad/1.0.0"
	default:
		return ""
	}
}

// SupportedProjects returns the list of projects that have known bootstrap
// peers and DHT protocol configuration.
func SupportedProjects() []string {
	return []string{"ipfs", "celestia"}
}

// ipfsBootstrapPeers are the well-known IPFS Amino DHT bootstrap nodes.
var ipfsBootstrapPeers = []string{
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
	"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
	"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
}

// celestiaBootstrapPeers — placeholder values. Replace with real Celestia
// bootstrap peers before production deployment.
var celestiaBootstrapPeers = []string{
	"/ip4/1.2.3.4/tcp/2121/p2p/12D3KooWDUMQDummyCelestiaBootstrap00000000000000",
}
