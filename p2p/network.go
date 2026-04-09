package p2p

// Network identifies a specific chain or testnet within a project.
type Network string

const (
	NetworkMainnet   Network = "mainnet"
	NetworkAmino     Network = "amino"
	NetworkCalibnet  Network = "calibnet"
	NetworkKusama    Network = "kusama"
	NetworkRococo    Network = "rococo"
	NetworkWestend   Network = "westend"
	NetworkArabica   Network = "arabica"
	NetworkMocha     Network = "mocha"
	NetworkBlockRace Network = "blockspace_race"
	NetworkConsensus Network = "consensus"
	NetworkExecution Network = "execution"
	NetworkHolesky   Network = "holesky"
	NetworkMainnetFN Network = "mainnet_fn"
	NetworkMainnetLC Network = "mainnet_lc"
	NetworkTuringFN  Network = "turing_fn"
	NetworkTuringLC  Network = "turing_lc"
	NetworkStatus    Network = "status"
	NetworkTWN       Network = "twn"
)
