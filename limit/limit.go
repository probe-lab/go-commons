// Package limit adapts the number of tasks that a process runs at the same
// time to the resources of its machine.
//
// A [Controller] keeps the limit well below what the machine can take. It
// projects, for every resource with a known size, the limit at which the
// tasks would use a target share of it (half by default), and approaches the
// lowest of these ceilings from below. A pressure signal that rises before
// anything fails, for example the time goroutines wait for a CPU, stops the
// growth. A damage signal, for example a dropped packet, cuts the limit and
// caps it below that value for the rest of the run.
//
// [System] reads these signals from the operating system and the Go runtime.
// A process can add its own signals to a [Reading].
package limit

// Names of the signals that [System] reports.
const (
	// Capacities
	SignalFiles              = "files"               // open file descriptors vs. RLIMIT_NOFILE
	SignalMemory             = "memory"              // working set vs. the cgroup memory limit, or the machine's memory
	SignalCPU                = "cpu"                 // CPU cores in use vs. the cgroup CPU quota or GOMAXPROCS
	SignalThreads            = "threads"             // OS threads vs. Go's thread limit (debug.SetMaxThreads)
	SignalPids               = "pids"                // tasks in the cgroup vs. pids.max
	SignalUDPPorts           = "udp_ports"           // UDP sockets vs. the ephemeral port range
	SignalTCPMemory          = "tcp_memory"          // kernel TCP buffer pages vs. the tcp_mem pressure threshold
	SignalUDPMemory          = "udp_memory"          // kernel UDP buffer pages vs. the udp_mem pressure threshold
	SignalConntrack          = "conntrack"           // netfilter connection tracking entries vs. nf_conntrack_max
	SignalConntrackAllowance = "conntrack_allowance" // AWS ENA connections tracked vs. the instance allowance

	// Pressures
	SignalSchedLatency = "sched_latency" // p99 time goroutines waited for a CPU, in seconds
	SignalCPUThrottled = "cpu_throttled" // share of cgroup CPU periods that were throttled
	SignalCPUStall     = "cpu_stall"     // share of time the cgroup waited for a CPU (PSI some avg10)
	SignalMemoryStall  = "memory_stall"  // share of time the cgroup waited for memory (PSI some avg10)
	SignalUDPQueue     = "udp_queue"     // fullest receive buffer of the process's UDP sockets

	// Damages
	SignalUDPDrops          = "udp_drops"           // datagrams that the process's UDP sockets dropped
	SignalUDPErrors         = "udp_errors"          // UDP send buffer and memory errors
	SignalTCPDrops          = "tcp_drops"           // TCP segments dropped on receive: backlog, receive queue, out-of-order queue
	SignalTCPMemoryPressure = "tcp_memory_pressure" // times TCP entered memory pressure or aborted a connection for lack of memory
	SignalBWInExceeded      = "bw_in_allowance_exceeded"
	SignalBWOutExceeded     = "bw_out_allowance_exceeded"
	SignalPPSExceeded       = "pps_allowance_exceeded"
	SignalConntrackExceeded = "conntrack_allowance_exceeded"
	SignalLinkLocalExceeded = "linklocal_allowance_exceeded"
)

// A Reading holds the signals that bound the limit at one point in time.
type Reading struct {
	Capacities []Capacity
	Pressures  []Pressure
	Damages    []Damage

	// Missing names the signals that could not be read.
	Missing []string
}

// Capacity is the use of a resource with a known size, for example the open
// file descriptors and the open-file limit.
type Capacity struct {
	Name string

	// Used is how much of the resource is in use.
	Used float64

	// Size is how much of the resource there is.
	Size float64

	// Target is the share of Size that the limit plans to use, more than 0
	// and at most 1.
	Target float64
}

// Share returns the share of the capacity in use.
func (c Capacity) Share() float64 {
	if c.Size <= 0 {
		return 0
	}
	return c.Used / c.Size
}

// Pressure is a value that rises with the load before anything fails, for
// example the time that goroutines wait for a CPU.
type Pressure struct {
	Name  string
	Value float64

	// Soft is the value from which on the limit stops growing. Zero turns
	// it off.
	Soft float64

	// Hard is the value from which on the limit shrinks. Zero turns it off.
	Hard float64
}

// Damage counts events that must not happen, for example dropped packets,
// since the previous reading.
type Damage struct {
	Name  string
	Count uint64
}

// Merge adds the signals of other to r.
func (r *Reading) Merge(other Reading) {
	r.Capacities = append(r.Capacities, other.Capacities...)
	r.Pressures = append(r.Pressures, other.Pressures...)
	r.Damages = append(r.Damages, other.Damages...)
	r.Missing = append(r.Missing, other.Missing...)
}

// Capacity returns the capacity with the given name.
func (r Reading) Capacity(name string) (Capacity, bool) {
	for _, c := range r.Capacities {
		if c.Name == name {
			return c, true
		}
	}
	return Capacity{}, false
}

// Pressure returns the pressure with the given name.
func (r Reading) Pressure(name string) (Pressure, bool) {
	for _, p := range r.Pressures {
		if p.Name == name {
			return p, true
		}
	}
	return Pressure{}, false
}

// Damage returns the damage with the given name.
func (r Reading) Damage(name string) (Damage, bool) {
	for _, d := range r.Damages {
		if d.Name == name {
			return d, true
		}
	}
	return Damage{}, false
}
