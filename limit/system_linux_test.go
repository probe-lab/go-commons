//go:build linux

package limit

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const udpHeader = "   sl  local_address rem_address   st tx_queue rx_queue tr tm->when retrnsmt   uid  timeout inode ref pointer drops\n"

func udpLine(inode string, rxQueue string, drops string) string {
	return "  1: 00000000:14E9 00000000:0000 07 00000000:" + rxQueue + " 00:00000000 00000000 0 0 " + inode + " 2 0000000000000000 " + drops + "\n"
}

// fakeReader returns an osReader on a fake tree of a cgroup v2 container
// with 2 CPUs and 4 GiB of memory.
func fakeReader(t *testing.T, files map[string]string) (*osReader, string, *time.Time, *time.Duration) {
	t.Helper()
	root := t.TempDir()

	tree := map[string]string{
		"proc/self/cgroup":                      "0::/\n",
		"proc/self/status":                      "Name:\tnebula\nVmRSS:\t1048576 kB\nThreads:\t50\n",
		"proc/self/fd/0":                        "->/dev/null",
		"proc/self/fd/3":                        "->socket:[100]",
		"proc/self/fd/4":                        "->socket:[200]",
		"proc/self/fd/5":                        "->socket:[300]",
		"proc/meminfo":                          "MemTotal: 16777216 kB\nMemAvailable: 8388608 kB\n",
		"proc/net/sockstat":                     "TCP: inuse 5 orphan 0 tw 2 alloc 6 mem 100\nUDP: inuse 1000 mem 50\n",
		"proc/net/sockstat6":                    "UDP6: inuse 400\n",
		"proc/sys/net/ipv4/ip_local_port_range": "32768\t60999\n",
		"proc/sys/net/ipv4/tcp_mem":             "1000 2000 3000\n",
		"proc/sys/net/ipv4/udp_mem":             "1000 2000 3000\n",
		"proc/net/udp":                          udpHeader + udpLine("100", "00000000", "0") + udpLine("999", "00010000", "50"),
		"proc/net/udp6":                         udpHeader + udpLine("200", "00000000", "3"),
		"proc/net/snmp":                         snmp,
		"proc/net/snmp6":                        "Udp6SndbufErrors 1\nUdp6MemErrors 0\n",
		"proc/net/netstat":                      netstat,
		"sys/fs/cgroup/cgroup.controllers":      "cpu memory pids\n",
		"sys/fs/cgroup/memory.max":              "4294967296\n",
		"sys/fs/cgroup/memory.current":          "1073741824\n",
		"sys/fs/cgroup/memory.stat":             "anon 1\ninactive_file 268435456\n",
		"sys/fs/cgroup/memory.pressure":         "some avg10=0.50 avg60=0.10 avg300=0.00 total=1\n",
		"sys/fs/cgroup/cpu.max":                 "200000 100000\n",
		"sys/fs/cgroup/cpu.stat":                "usage_usec 1\nnr_periods 100\nnr_throttled 10\n",
		"sys/fs/cgroup/cpu.pressure":            "some avg10=2.00 avg60=1.00 avg300=0.00 total=1\n",
		"sys/fs/cgroup/pids.max":                "4096\n",
		"sys/fs/cgroup/pids.current":            "60\n",
	}
	for k, v := range files {
		tree[k] = v
	}
	writeTree(t, root, tree)

	now := t0
	cpu := time.Duration(0)
	o := &osReader{
		proc:       filepath.Join(root, "proc"),
		sys:        filepath.Join(root, "sys"),
		target:     0.5,
		now:        func() time.Time { return now },
		fileLimit:  func() (uint64, error) { return 1024, nil },
		cpuTime:    func() (time.Duration, error) { return cpu, nil },
		rcvbuf:     func(fd int) (int, error) { return 0x20000, nil },
		maxThreads: 10000,
	}
	o.init()

	return o, root, &now, &cpu
}

func TestOSReader_Capacities(t *testing.T) {
	o, _, now, cpu := fakeReader(t, nil)

	*now = now.Add(5 * time.Second)
	*cpu += 5 * time.Second // one CPU for 5 seconds

	var r Reading
	o.read(&r)
	assert.Empty(t, r.Missing)

	want := map[string][2]float64{
		SignalFiles:     {4, 1024},
		SignalMemory:    {1 << 30 * 3 / 4, 1 << 32},
		SignalCPU:       {1, min(2, float64(runtime.GOMAXPROCS(0)))},
		SignalThreads:   {50, 10000},
		SignalPids:      {60, 4096},
		SignalUDPPorts:  {1000, 28232},
		SignalTCPMemory: {100, 2000},
		SignalUDPMemory: {50, 2000},
	}
	for name, usedSize := range want {
		c, ok := r.Capacity(name)
		if assert.True(t, ok, name) {
			assert.InDelta(t, usedSize[0], c.Used, 1e-6, name)
			assert.InDelta(t, usedSize[1], c.Size, 1e-6, name)
			assert.Equal(t, 0.5, c.Target, name)
		}
	}
}

func TestOSReader_Pressures(t *testing.T) {
	o, root, now, _ := fakeReader(t, nil)

	require.NoError(t, writeFile(root, "sys/fs/cgroup/cpu.stat", "nr_periods 200\nnr_throttled 30\n"))
	// socket 100 has 0x8000 bytes queued in a buffer of 0x20000
	require.NoError(t, writeFile(root, "proc/net/udp", udpHeader+udpLine("100", "00008000", "0")))
	*now = now.Add(5 * time.Second)

	var r Reading
	o.read(&r)

	p, ok := r.Pressure(SignalCPUThrottled)
	require.True(t, ok)
	assert.InDelta(t, 0.2, p.Value, 1e-9, "20 of 100 periods")

	p, ok = r.Pressure(SignalUDPQueue)
	require.True(t, ok)
	assert.InDelta(t, 0.25, p.Value, 1e-9)

	p, ok = r.Pressure(SignalCPUStall)
	require.True(t, ok)
	assert.InDelta(t, 0.02, p.Value, 1e-9)

	p, ok = r.Pressure(SignalMemoryStall)
	require.True(t, ok)
	assert.InDelta(t, 0.005, p.Value, 1e-9)
}

func TestOSReader_Damages(t *testing.T) {
	o, root, now, _ := fakeReader(t, nil)

	var r Reading
	o.read(&r)
	for _, d := range r.Damages {
		assert.Zero(t, d.Count, "no change since the start: %s", d.Name)
	}

	// socket 200 dropped 4 more datagrams, socket 999 is not ours
	require.NoError(t, writeFile(root, "proc/net/udp6", udpHeader+udpLine("200", "00000000", "7")))
	require.NoError(t, writeFile(root, "proc/net/udp", udpHeader+udpLine("100", "00000000", "0")+udpLine("999", "00000000", "80")))
	require.NoError(t, writeFile(root, "proc/net/snmp6", "Udp6SndbufErrors 3\nUdp6MemErrors 1\n"))
	require.NoError(t, writeFile(root, "proc/net/netstat", "TcpExt: TCPBacklogDrop TCPRcvQDrop TCPOFODrop TCPMemoryPressures TCPAbortOnMemory\nTcpExt: 5 2 1 6 2\n"))
	*now = now.Add(5 * time.Second)

	r = Reading{}
	o.read(&r)

	counts := map[string]uint64{}
	for _, d := range r.Damages {
		counts[d.Name] = d.Count
	}
	assert.Equal(t, map[string]uint64{
		SignalUDPDrops:          4,
		SignalUDPErrors:         3,
		SignalTCPDrops:          1,
		SignalTCPMemoryPressure: 2,
	}, counts)

	// a counter that goes down was reset
	require.NoError(t, writeFile(root, "proc/net/netstat", "TcpExt: TCPBacklogDrop TCPRcvQDrop TCPOFODrop TCPMemoryPressures TCPAbortOnMemory\nTcpExt: 0 0 0 0 0\n"))
	r = Reading{}
	o.read(&r)
	d, _ := r.Damage(SignalTCPDrops)
	assert.Zero(t, d.Count)
}

func TestOSReader_MemoryWithoutCgroupLimit(t *testing.T) {
	o, _, _, _ := fakeReader(t, map[string]string{"sys/fs/cgroup/memory.max": "max\n"})

	var r Reading
	o.read(&r)
	// other processes use the memory that is not available: only the
	// resident memory of the process counts
	c, ok := r.Capacity(SignalMemory)
	require.True(t, ok)
	assert.Equal(t, float64(1<<30), c.Used, "VmRSS")
	assert.Equal(t, float64(9<<30), c.Size, "VmRSS + MemAvailable")
}

func TestOSReader_NoUDPSockets(t *testing.T) {
	o, root, _, _ := fakeReader(t, nil)
	require.NoError(t, writeFile(root, "proc/net/udp", udpHeader))
	require.NoError(t, os.Remove(filepath.Join(root, "proc/net/udp6")))

	var r Reading
	o.read(&r)
	assert.NotContains(t, r.Missing, SignalUDPDrops, "an empty table is no missing table")
	d, ok := r.Damage(SignalUDPDrops)
	assert.True(t, ok)
	assert.Zero(t, d.Count)
}

func TestOSReader_ENA(t *testing.T) {
	o, _, _, _ := fakeReader(t, nil)

	stats := map[string]uint64{
		"pps_allowance_exceeded":        10,
		"conntrack_allowance_available": 100000,
		"rx_packets":                    1,
	}
	o.ena = func() (map[string]uint64, bool) { return stats, true }
	o.init()

	stats = map[string]uint64{
		"pps_allowance_exceeded":        12,
		"conntrack_allowance_available": 60000,
	}
	var r Reading
	o.read(&r)

	d, ok := r.Damage(SignalPPSExceeded)
	require.True(t, ok)
	assert.Equal(t, uint64(2), d.Count)

	c, ok := r.Capacity(SignalConntrackAllowance)
	require.True(t, ok)
	assert.Equal(t, 40000.0, c.Used)
	assert.Equal(t, 100000.0, c.Size)
}

func TestOSReader_Missing(t *testing.T) {
	root := t.TempDir()
	o := &osReader{
		proc:      filepath.Join(root, "proc"),
		sys:       filepath.Join(root, "sys"),
		target:    0.5,
		now:       time.Now,
		fileLimit: func() (uint64, error) { return 1024, nil },
		cpuTime:   func() (time.Duration, error) { return 0, assert.AnError },
		rcvbuf:    func(int) (int, error) { return 0, assert.AnError },
	}
	o.init()

	var r Reading
	o.read(&r)
	assert.Subset(t, r.Missing, []string{SignalFiles, SignalMemory, SignalCPU, SignalThreads, SignalUDPDrops, SignalTCPDrops})
	assert.Empty(t, r.Capacities)
}

// TestSystem_Read reads the machine the test runs on.
func TestSystem_Read(t *testing.T) {
	conn, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	s, err := NewSystem(DefaultSystemConfig())
	require.NoError(t, err)
	time.Sleep(10 * time.Millisecond)

	r := s.Read()
	for _, name := range []string{SignalFiles, SignalMemory, SignalCPU, SignalThreads} {
		c, ok := r.Capacity(name)
		if assert.True(t, ok, name) {
			assert.Positive(t, c.Size, name)
		}
	}
	files, _ := r.Capacity(SignalFiles)
	assert.GreaterOrEqual(t, files.Used, 1.0, "the UDP socket")

	_, ok := r.Pressure(SignalSchedLatency)
	assert.True(t, ok)
	_, ok = r.Damage(SignalUDPDrops)
	assert.True(t, ok)

	_, err = NewSystem(SystemConfig{Target: 0})
	assert.Error(t, err)
}

func writeFile(root, name, content string) error {
	return os.WriteFile(filepath.Join(root, name), []byte(content), 0o644)
}
