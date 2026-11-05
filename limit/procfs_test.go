package limit

import (
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const snmp = `Ip: Forwarding DefaultTTL InReceives
Ip: 1 64 12345
Tcp: RtoAlgorithm RtoMin MaxConn ActiveOpens
Tcp: 1 200 -1 77
Udp: InDatagrams NoPorts InErrors OutDatagrams RcvbufErrors SndbufErrors InCsumErrors IgnoredMulti MemErrors
Udp: 100 2 7 90 5 3 0 0 1
`

const netstat = `TcpExt: SyncookiesSent TCPBacklogDrop TCPRcvQDrop TCPOFODrop TCPMemoryPressures TCPAbortOnMemory
TcpExt: 0 4 2 1 6 0
IpExt: InNoRoutes InOctets
IpExt: 0 999
`

func TestParseTable(t *testing.T) {
	table := parseTable(snmp)
	assert.Equal(t, uint64(3), table["Udp"]["SndbufErrors"])
	assert.Equal(t, uint64(1), table["Udp"]["MemErrors"])
	assert.Equal(t, uint64(77), table["Tcp"]["ActiveOpens"])
	_, ok := table["Tcp"]["MaxConn"]
	assert.False(t, ok, "negative values are no counters")

	ext := parseTable(netstat)
	assert.Equal(t, uint64(4), ext["TcpExt"]["TCPBacklogDrop"])
	assert.Equal(t, uint64(999), ext["IpExt"]["InOctets"])
}

func TestParseKeyValues(t *testing.T) {
	values := parseKeyValues("Udp6InDatagrams   12\nUdp6SndbufErrors  3\nnr_periods 10\nbroken\n")
	assert.Equal(t, map[string]uint64{"Udp6InDatagrams": 12, "Udp6SndbufErrors": 3, "nr_periods": 10}, values)
}

func TestParseSockstat(t *testing.T) {
	stat := parseSockstat("sockets: used 290\nTCP: inuse 5 orphan 0 tw 2 alloc 6 mem 1\nUDP: inuse 3 mem 4\nUDPLITE: inuse 0\n")
	assert.Equal(t, uint64(290), stat["sockets"]["used"])
	assert.Equal(t, uint64(2), stat["TCP"]["tw"])
	assert.Equal(t, uint64(4), stat["UDP"]["mem"])
}

func TestParseUDPSockets(t *testing.T) {
	content := `   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops
  204: 00000000:14E9 00000000:0000 07 00000000:00000000 00:00000000 00000000   101        0 18574 2 0000000000000000 0
  988: 00000000:D2C4 00000000:0000 07 00000000:00001A00 00:00000000 00000000     0        0 41234 2 0000000000000000 17
`
	sockets, err := parseUDPSockets(content)
	require.NoError(t, err)
	assert.Equal(t, []udpSocket{
		{inode: 18574},
		{inode: 41234, rxQueue: 0x1A00, drops: 17},
	}, sockets)
}

func TestParseStatusField(t *testing.T) {
	threads, err := parseStatusField("Name:\tnebula\nThreads:\t42\nVmRSS:\t1024 kB\n", "Threads")
	require.NoError(t, err)
	assert.Equal(t, uint64(42), threads)

	_, err = parseStatusField("Name:\tnebula\n", "Threads")
	assert.Error(t, err)
}

func TestParseMeminfo(t *testing.T) {
	m := parseMeminfo("MemTotal:       16000000 kB\nMemAvailable:    4000000 kB\nHugePages_Total:       0\n")
	assert.Equal(t, uint64(16000000*1024), m["MemTotal"])
	assert.Equal(t, uint64(4000000*1024), m["MemAvailable"])
	assert.Equal(t, uint64(0), m["HugePages_Total"])
}

func TestParsePSI(t *testing.T) {
	share, err := parsePSI("some avg10=12.50 avg60=3.00 avg300=1.00 total=123\nfull avg10=1.00 avg60=0.00 avg300=0.00 total=12\n")
	require.NoError(t, err)
	assert.InDelta(t, 0.125, share, 1e-9)

	_, err = parsePSI("full avg10=1.00\n")
	assert.Error(t, err)
}

func TestParseLimits(t *testing.T) {
	v, ok, err := parseLimit("max\n")
	require.NoError(t, err)
	assert.False(t, ok)
	assert.Zero(t, v)

	v, ok, err = parseLimit("4294967296\n")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.Equal(t, uint64(4294967296), v)

	cpus, ok, err := parseCPUMax("250000 100000\n")
	require.NoError(t, err)
	assert.True(t, ok)
	assert.InDelta(t, 2.5, cpus, 1e-9)

	_, ok, err = parseCPUMax("max 100000\n")
	require.NoError(t, err)
	assert.False(t, ok)

	_, _, err = parseCPUMax("garbage")
	assert.Error(t, err)

	values, err := parseUints("32768\t60999\n")
	require.NoError(t, err)
	assert.Equal(t, []uint64{32768, 60999}, values)
}

func TestParseSocketLink(t *testing.T) {
	inode, ok := parseSocketLink("socket:[41234]")
	assert.True(t, ok)
	assert.Equal(t, uint64(41234), inode)

	for _, link := range []string{"/dev/null", "pipe:[123]", "socket:[x]", "socket:[12"} {
		_, ok := parseSocketLink(link)
		assert.False(t, ok, link)
	}
}

func TestPercentile(t *testing.T) {
	buckets := []float64{math.Inf(-1), 0.001, 0.002, 0.004, math.Inf(1)}

	assert.Zero(t, percentile(buckets, []uint64{0, 0, 0, 0}, 0.99))
	assert.Equal(t, 0.002, percentile(buckets, []uint64{0, 100, 0, 0}, 0.99))
	assert.Equal(t, 0.004, percentile(buckets, []uint64{0, 98, 1, 1}, 0.99))
	assert.Equal(t, 0.004, percentile(buckets, []uint64{0, 0, 0, 5}, 0.99), "the last bucket has no upper bound")
	assert.Zero(t, percentile(buckets, []uint64{1}, 0.99), "mismatched buckets")
}
