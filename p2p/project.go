package p2p

// Project identifies a blockchain ecosystem.
type Project string

const (
	ProjectIPFS     Project = "ipfs"
	ProjectFilecoin Project = "filecoin"
	ProjectCelestia Project = "celestia"
	ProjectEthereum Project = "ethereum"
	ProjectPolkadot Project = "polkadot"
	ProjectAvail    Project = "avail"
	ProjectGnosis   Project = "gnosis"
	ProjectPactus   Project = "pactus"
	ProjectBitcoin  Project = "bitcoin"
	ProjectDria     Project = "dria"
	ProjectWaku     Project = "waku"
	ProjectMonero   Project = "monero"
	ProjectAlgorand Project = "algorand"
	ProjectPortal   Project = "portal"
)
