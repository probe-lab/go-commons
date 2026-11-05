//go:build linux

package limit

import (
	"math"
	"os"
	"path/filepath"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"syscall"
	"time"
)

// osReader reads the signals of a Linux machine.
type osReader struct {
	// proc and sys are the mount points of procfs and sysfs. Tests set
	// fake trees.
	proc string
	sys  string

	target float64
	cgroup cgroup

	// The system calls. Tests replace them.
	now       func() time.Time
	fileLimit func() (uint64, error)
	cpuTime   func() (time.Duration, error)
	rcvbuf    func(fd int) (int, error)
	ena       func() (map[string]uint64, bool)

	// maxThreads is the thread limit of the Go runtime.
	maxThreads int

	// The state of the previous read.
	prevTime      time.Time
	prevCPU       time.Duration
	prevCPUOK     bool
	prevThrottle  [2]uint64 // nr_periods, nr_throttled
	prevThrottled bool
	counters      map[string]uint64 // damage counters by signal
	udpDrops      map[uint64]uint64 // drops by socket inode

	// conntrackAllowance is the ENA connection tracking allowance that
	// was available at the start. Zero means unknown.
	conntrackAllowance float64
}

func newOSReader(target float64) *osReader {
	o := &osReader{
		proc:       "/proc",
		sys:        "/sys",
		target:     target,
		now:        time.Now,
		fileLimit:  fileLimit,
		cpuTime:    cpuTime,
		rcvbuf:     rcvbuf,
		ena:        enaReader(),
		maxThreads: maxThreads(),
	}
	o.init()
	return o
}

// init finds the cgroup and reads the counters that later reads report the
// change of.
func (o *osReader) init() {
	o.cgroup = findCgroup(o.proc, o.sys)
	o.counters = map[string]uint64{}
	o.udpDrops = map[uint64]uint64{}
	o.read(&Reading{})
}

func (o *osReader) read(r *Reading) {
	now := o.now()
	elapsed := now.Sub(o.prevTime)
	o.prevTime = now

	sockets := o.readFiles(r)
	o.readMemory(r)
	o.readCPU(r, elapsed)
	o.readThreads(r)
	o.readSockets(r)
	o.readUDP(r, sockets)
	o.readTCP(r)
	o.readConntrack(r)
	o.readAllowances(r)
}

// readFiles reports the open file descriptors and returns the descriptors of
// the process's sockets by inode.
func (o *osReader) readFiles(r *Reading) map[uint64]int {
	dir := filepath.Join(o.proc, "self", "fd")
	f, err := os.Open(dir)
	if err != nil {
		r.Missing = append(r.Missing, SignalFiles)
		return nil
	}
	names, err := f.Readdirnames(-1)
	_ = f.Close()
	if err != nil {
		r.Missing = append(r.Missing, SignalFiles)
		return nil
	}

	sockets := map[uint64]int{}
	for _, name := range names {
		fd, err := strconv.Atoi(name)
		if err != nil {
			continue
		}
		link, err := os.Readlink(filepath.Join(dir, name))
		if err != nil {
			continue
		}
		if inode, ok := parseSocketLink(link); ok {
			sockets[inode] = fd
		}
	}

	limit, err := o.fileLimit()
	if err != nil || limit == 0 || limit >= math.MaxInt64 {
		r.Missing = append(r.Missing, SignalFiles)
		return sockets
	}

	r.Capacities = append(r.Capacities, o.capacity(SignalFiles, float64(len(names)), float64(limit)))

	return sockets
}

// readMemory reports the working set of the cgroup against its limit. The
// working set leaves out inactive file pages, which the kernel can reclaim;
// it includes the kernel's socket buffers. Without a cgroup limit, other
// processes share the machine's memory, so it reports the resident memory of
// the process against what the process could have: its resident memory and
// the memory still available on the machine.
func (o *osReader) readMemory(r *Reading) {
	if used, size, ok := o.cgroupMemory(); ok {
		r.Capacities = append(r.Capacities, o.capacity(SignalMemory, used, size))
	} else if used, size, ok := o.processMemory(); ok {
		r.Capacities = append(r.Capacities, o.capacity(SignalMemory, used, size))
	} else {
		r.Missing = append(r.Missing, SignalMemory)
	}

	if o.cgroup.v2 && o.cgroup.memory != "" {
		if psi, err := readFile(o.cgroup.memory, "memory.pressure"); err == nil {
			if share, err := parsePSI(psi); err == nil {
				r.Pressures = append(r.Pressures, Pressure{Name: SignalMemoryStall, Value: share, Soft: memoryStallSoft, Hard: memoryStallHard})
			}
		}
	}
}

// processMemory returns the resident memory of the process and the memory
// it could have.
func (o *osReader) processMemory() (used, size float64, ok bool) {
	meminfo, err := o.readProc("meminfo")
	if err != nil {
		return 0, 0, false
	}
	available, ok := parseMeminfo(meminfo)["MemAvailable"]
	if !ok {
		return 0, 0, false
	}

	status, err := o.readProc("self/status")
	if err != nil {
		return 0, 0, false
	}
	rss, ok := parseMeminfo(status)["VmRSS"]
	if !ok || rss == 0 {
		return 0, 0, false
	}

	return float64(rss), float64(rss + available), true
}

// cgroupMemory returns the working set and the memory limit of the cgroup.
// It returns false if the cgroup has no limit.
func (o *osReader) cgroupMemory() (used, size float64, ok bool) {
	dir := o.cgroup.memory
	if dir == "" {
		return 0, 0, false
	}

	currentFile, limitFile, inactiveKey := "memory.current", "memory.max", "inactive_file"
	if !o.cgroup.v2 {
		currentFile, limitFile, inactiveKey = "memory.usage_in_bytes", "memory.limit_in_bytes", "total_inactive_file"
	}

	limitContent, err := readFile(dir, limitFile)
	if err != nil {
		return 0, 0, false
	}
	limit, limited, err := parseLimit(limitContent)
	if err != nil || !limited {
		return 0, 0, false
	}

	// cgroup v1 reports no limit as a number near 2^63. A limit above the
	// machine's memory is none either.
	if meminfo, err := o.readProc("meminfo"); err == nil {
		if total := parseMeminfo(meminfo)["MemTotal"]; total > 0 && limit >= total {
			return 0, 0, false
		}
	}

	currentContent, err := readFile(dir, currentFile)
	if err != nil {
		return 0, 0, false
	}
	current, _, err := parseLimit(currentContent)
	if err != nil {
		return 0, 0, false
	}

	if stat, err := readFile(dir, "memory.stat"); err == nil {
		if inactive := parseKeyValues(stat)[inactiveKey]; inactive <= current {
			current -= inactive
		}
	}

	return float64(current), float64(limit), true
}

// readCPU reports the CPUs the process used since the previous read against
// the CPUs it can use: the cgroup quota or GOMAXPROCS, whichever is lower. It
// also reports the share of throttled CPU periods and the CPU stall of the
// cgroup.
func (o *osReader) readCPU(r *Reading, elapsed time.Duration) {
	cpu, err := o.cpuTime()
	prevCPU, prevOK := o.prevCPU, o.prevCPUOK
	o.prevCPU, o.prevCPUOK = cpu, err == nil

	size := float64(runtime.GOMAXPROCS(0))
	if quota, ok := o.cpuQuota(); ok {
		size = min(size, quota)
	}

	if err != nil || !prevOK || elapsed <= 0 || size <= 0 {
		r.Missing = append(r.Missing, SignalCPU)
	} else {
		used := max(float64(cpu-prevCPU)/float64(elapsed), 0)
		r.Capacities = append(r.Capacities, o.capacity(SignalCPU, used, size))
	}

	if o.cgroup.cpu == "" {
		return
	}

	if stat, err := readFile(o.cgroup.cpu, "cpu.stat"); err == nil {
		values := parseKeyValues(stat)
		periods, pok := values["nr_periods"]
		throttled, tok := values["nr_throttled"]
		if pok && tok {
			prev, hadPrev := o.prevThrottle, o.prevThrottled
			o.prevThrottle, o.prevThrottled = [2]uint64{periods, throttled}, true
			if hadPrev && periods > prev[0] && throttled >= prev[1] {
				share := float64(throttled-prev[1]) / float64(periods-prev[0])
				r.Pressures = append(r.Pressures, Pressure{Name: SignalCPUThrottled, Value: share, Soft: cpuThrottledSoft, Hard: cpuThrottledHard})
			}
		}
	}

	if o.cgroup.v2 {
		if psi, err := readFile(o.cgroup.cpu, "cpu.pressure"); err == nil {
			if share, err := parsePSI(psi); err == nil {
				r.Pressures = append(r.Pressures, Pressure{Name: SignalCPUStall, Value: share, Soft: cpuStallSoft, Hard: cpuStallHard})
			}
		}
	}
}

// cpuQuota returns the CPU quota of the cgroup in CPUs.
func (o *osReader) cpuQuota() (float64, bool) {
	dir := o.cgroup.cpu
	if dir == "" {
		return 0, false
	}

	if o.cgroup.v2 {
		content, err := readFile(dir, "cpu.max")
		if err != nil {
			return 0, false
		}
		quota, ok, err := parseCPUMax(content)
		return quota, ok && err == nil && quota > 0
	}

	quotaContent, err := readFile(dir, "cpu.cfs_quota_us")
	if err != nil {
		return 0, false
	}
	periodContent, err := readFile(dir, "cpu.cfs_period_us")
	if err != nil {
		return 0, false
	}
	quota, err := strconv.ParseFloat(strings.TrimSpace(quotaContent), 64)
	if err != nil || quota <= 0 {
		return 0, false // -1: no quota
	}
	period, err := strconv.ParseFloat(strings.TrimSpace(periodContent), 64)
	if err != nil || period <= 0 {
		return 0, false
	}
	return quota / period, true
}

// readThreads reports the OS threads of the process against the thread limit
// of the Go runtime, which crashes the process when it is reached, and the
// tasks of the cgroup against its pids limit.
func (o *osReader) readThreads(r *Reading) {
	status, err := o.readProc("self/status")
	if err != nil {
		r.Missing = append(r.Missing, SignalThreads)
	} else if threads, err := parseStatusField(status, "Threads"); err != nil || o.maxThreads <= 0 {
		r.Missing = append(r.Missing, SignalThreads)
	} else {
		r.Capacities = append(r.Capacities, o.capacity(SignalThreads, float64(threads), float64(o.maxThreads)))
	}

	if o.cgroup.pids == "" {
		return
	}
	limitContent, err := readFile(o.cgroup.pids, "pids.max")
	if err != nil {
		return
	}
	limit, limited, err := parseLimit(limitContent)
	if err != nil || !limited {
		return
	}
	currentContent, err := readFile(o.cgroup.pids, "pids.current")
	if err != nil {
		return
	}
	current, _, err := parseLimit(currentContent)
	if err != nil {
		return
	}
	r.Capacities = append(r.Capacities, o.capacity(SignalPids, float64(current), float64(limit)))
}

// readSockets reports the UDP sockets of the network namespace against the
// ephemeral port range, and the kernel memory of TCP and UDP buffers against
// the thresholds at which the kernel starts to limit them. Every UDP socket
// that binds no port takes one from the ephemeral range.
func (o *osReader) readSockets(r *Reading) {
	sockstat := map[string]map[string]uint64{}
	if content, err := o.readProc("net/sockstat"); err == nil {
		sockstat = parseSockstat(content)
	}
	if content, err := o.readProc("net/sockstat6"); err == nil {
		for k, v := range parseSockstat(content) {
			sockstat[k] = v
		}
	}

	if content, err := o.readProc("sys/net/ipv4/ip_local_port_range"); err == nil {
		if bounds, err := parseUints(content); err == nil && len(bounds) == 2 && bounds[1] >= bounds[0] {
			ports := float64(bounds[1] - bounds[0] + 1)
			udp4, ok4 := sockstat["UDP"]["inuse"]
			udp6, ok6 := sockstat["UDP6"]["inuse"]
			if ok4 || ok6 {
				// IPv4 and IPv6 sockets take ports from separate spaces.
				r.Capacities = append(r.Capacities, o.capacity(SignalUDPPorts, float64(max(udp4, udp6)), ports))
			}
		}
	}

	memory := func(name, proto, sysctl string) {
		pages, ok := sockstat[proto]["mem"]
		if !ok {
			return
		}
		content, err := o.readProc("sys/net/ipv4/" + sysctl)
		if err != nil {
			return
		}
		// low, pressure, high, in pages
		thresholds, err := parseUints(content)
		if err != nil || len(thresholds) != 3 || thresholds[1] == 0 {
			return
		}
		r.Capacities = append(r.Capacities, o.capacity(name, float64(pages), float64(thresholds[1])))
	}
	memory(SignalTCPMemory, "TCP", "tcp_mem")
	memory(SignalUDPMemory, "UDP", "udp_mem")
}

// readUDP reports the drops and the fullest receive buffer of the process's
// UDP sockets, and the UDP send and memory errors of the network namespace.
func (o *osReader) readUDP(r *Reading, sockets map[uint64]int) {
	// A kernel without IPv6 has no udp6 table. An empty table is fine.
	var lines []udpSocket
	read := false
	for _, name := range []string{"net/udp", "net/udp6"} {
		content, err := o.readProc(name)
		if err != nil {
			continue
		}
		parsed, err := parseUDPSockets(content)
		if err != nil {
			continue
		}
		read = true
		lines = append(lines, parsed...)
	}

	if sockets == nil || !read {
		r.Missing = append(r.Missing, SignalUDPDrops, SignalUDPQueue)
	} else {
		var drops uint64
		fill := 0.0
		seen := make(map[uint64]uint64, len(sockets))
		for _, line := range lines {
			fd, ours := sockets[line.inode]
			if !ours {
				continue
			}
			seen[line.inode] = line.drops
			if prev := o.udpDrops[line.inode]; line.drops > prev {
				drops += line.drops - prev
			}
			if line.rxQueue == 0 {
				continue
			}
			if size, err := o.rcvbuf(fd); err == nil && size > 0 {
				fill = max(fill, float64(line.rxQueue)/float64(size))
			}
		}
		o.udpDrops = seen

		r.Damages = append(r.Damages, Damage{Name: SignalUDPDrops, Count: drops})
		r.Pressures = append(r.Pressures, Pressure{Name: SignalUDPQueue, Value: fill, Soft: udpQueueSoft, Hard: udpQueueHard})
	}

	var errs uint64
	ok := false
	if content, err := o.readProc("net/snmp"); err == nil {
		udp := parseTable(content)["Udp"]
		errs += udp["SndbufErrors"] + udp["MemErrors"]
		ok = udp != nil
	}
	if content, err := o.readProc("net/snmp6"); err == nil {
		udp6 := parseKeyValues(content)
		errs += udp6["Udp6SndbufErrors"] + udp6["Udp6MemErrors"]
		ok = true
	}
	if ok {
		r.Damages = append(r.Damages, o.damage(SignalUDPErrors, errs))
	}
}

// readTCP reports the TCP segments that the kernel dropped on receive
// because a socket's buffers were full, and the times TCP ran short of
// memory. Retransmissions are no signal: most of them come from dials to
// peers that are gone.
func (o *osReader) readTCP(r *Reading) {
	content, err := o.readProc("net/netstat")
	if err != nil {
		r.Missing = append(r.Missing, SignalTCPDrops)
		return
	}
	ext, ok := parseTable(content)["TcpExt"]
	if !ok {
		r.Missing = append(r.Missing, SignalTCPDrops)
		return
	}

	r.Damages = append(r.Damages,
		o.damage(SignalTCPDrops, ext["TCPBacklogDrop"]+ext["TCPRcvQDrop"]+ext["TCPOFODrop"]),
		o.damage(SignalTCPMemoryPressure, ext["TCPMemoryPressures"]+ext["TCPAbortOnMemory"]),
	)
}

// readConntrack reports the netfilter connection tracking entries against
// the table size, if the kernel tracks connections.
func (o *osReader) readConntrack(r *Reading) {
	count, err := o.readProc("sys/net/netfilter/nf_conntrack_count")
	if err != nil {
		return
	}
	size, err := o.readProc("sys/net/netfilter/nf_conntrack_max")
	if err != nil {
		return
	}
	c, err1 := strconv.ParseFloat(strings.TrimSpace(count), 64)
	s, err2 := strconv.ParseFloat(strings.TrimSpace(size), 64)
	if err1 != nil || err2 != nil || s <= 0 {
		return
	}
	r.Capacities = append(r.Capacities, o.capacity(SignalConntrack, c, s))
}

// enaExceeded are the ENA statistics that count packets that the network
// interface queued or dropped because the instance exceeded an allowance.
var enaExceeded = []string{
	SignalBWInExceeded,
	SignalBWOutExceeded,
	SignalPPSExceeded,
	SignalConntrackExceeded,
	SignalLinkLocalExceeded,
}

// readAllowances reports the allowances of the AWS ENA interfaces. ENA tells
// how many tracked connections are still available, not the allowance. The
// available connections at the first read stand in for the allowance.
func (o *osReader) readAllowances(r *Reading) {
	if o.ena == nil {
		return
	}
	stats, ok := o.ena()
	if !ok {
		return
	}

	for _, name := range enaExceeded {
		if v, ok := stats[name]; ok {
			r.Damages = append(r.Damages, o.damage(name, v))
		}
	}

	if available, ok := stats["conntrack_allowance_available"]; ok {
		if o.conntrackAllowance == 0 {
			o.conntrackAllowance = float64(available)
		}
		if o.conntrackAllowance > 0 {
			used := max(o.conntrackAllowance-float64(available), 0)
			r.Capacities = append(r.Capacities, o.capacity(SignalConntrackAllowance, used, o.conntrackAllowance))
		}
	}
}

// capacity returns a capacity with the reader's target.
func (o *osReader) capacity(name string, used, size float64) Capacity {
	return Capacity{Name: name, Used: used, Size: size, Target: o.target}
}

// damage returns the change of a counter since the previous read. A counter
// that went down was reset; its change is zero.
func (o *osReader) damage(name string, value uint64) Damage {
	prev, ok := o.counters[name]
	o.counters[name] = value
	if !ok || value < prev {
		return Damage{Name: name}
	}
	return Damage{Name: name, Count: value - prev}
}

func (o *osReader) readProc(name string) (string, error) {
	return readFile(o.proc, name)
}

func readFile(dir, name string) (string, error) {
	data, err := os.ReadFile(filepath.Join(dir, name))
	return string(data), err
}

func fileLimit() (uint64, error) {
	var rlimit syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rlimit); err != nil {
		return 0, err
	}
	return rlimit.Cur, nil
}

func cpuTime() (time.Duration, error) {
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		return 0, err
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano()), nil
}

// rcvbuf returns the receive buffer size of a socket of the process. The
// kernel drops a datagram when the queued datagrams would take more memory.
func rcvbuf(fd int) (int, error) {
	return syscall.GetsockoptInt(fd, syscall.SOL_SOCKET, syscall.SO_RCVBUF)
}

// maxThreads returns the thread limit of the Go runtime. The runtime only
// lets a program set it, so set a higher value and restore the old one.
func maxThreads() int {
	n := debug.SetMaxThreads(math.MaxInt32)
	debug.SetMaxThreads(n)
	return n
}
