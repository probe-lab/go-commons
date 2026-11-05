package limit

import (
	"fmt"
	"math"
	"runtime/metrics"
	"slices"
)

// The thresholds of the pressure signals that [System] reports. The soft
// thresholds sit well below the values at which tasks start to fail.
const (
	// The time goroutines wait for a CPU, p99, in seconds. A loaded process
	// waits for microseconds.
	schedLatencySoft = 0.005
	schedLatencyHard = 0.020

	// The share of the cgroup's CPU periods in which it ran out of quota.
	cpuThrottledSoft = 0.05
	cpuThrottledHard = 0.2

	// The share of time in which a task of the cgroup waited for a CPU or for
	// memory (pressure stall information, avg10).
	cpuStallSoft    = 0.1
	cpuStallHard    = 0.25
	memoryStallSoft = 0.01
	memoryStallHard = 0.05

	// The fill of the fullest UDP receive buffer. A full buffer drops
	// datagrams.
	udpQueueSoft = 0.25
	udpQueueHard = 0.5
)

// SystemConfig configures a [System].
type SystemConfig struct {
	// Target is the share of each capacity that the limit plans to use, more
	// than 0 and at most 1.
	Target float64
}

// DefaultSystemConfig returns the default configuration: the limit plans to
// use half of every resource.
func DefaultSystemConfig() SystemConfig {
	return SystemConfig{Target: 0.5}
}

// System reads the signals of the machine and the Go runtime. On Linux, it
// reads the open files, memory, CPU, threads, sockets, and drops of the
// process and its cgroup, and the allowances of AWS ENA interfaces. On other
// systems, it reads only the scheduling latency of the Go runtime, and the
// files, memory, and CPU capacities are missing. A System is not safe for
// concurrent use.
type System struct {
	sched *schedLatency
	os    *osReader
}

// NewSystem returns a System. Its first [System.Read] reports the signals
// since NewSystem.
func NewSystem(cfg SystemConfig) (*System, error) {
	if cfg.Target <= 0 || cfg.Target > 1 {
		return nil, fmt.Errorf("target must be more than 0 and at most 1, got %v", cfg.Target)
	}

	return &System{
		sched: newSchedLatency(),
		os:    newOSReader(cfg.Target),
	}, nil
}

// Read returns the signals since the previous call.
func (s *System) Read() Reading {
	var r Reading

	if p99, ok := s.sched.read(); ok {
		r.Pressures = append(r.Pressures, Pressure{
			Name:  SignalSchedLatency,
			Value: p99,
			Soft:  schedLatencySoft,
			Hard:  schedLatencyHard,
		})
	} else {
		r.Missing = append(r.Missing, SignalSchedLatency)
	}

	s.os.read(&r)

	return r
}

// schedLatency reads the time goroutines wait for a CPU from the runtime.
type schedLatency struct {
	samples []metrics.Sample
	prev    []uint64
}

func newSchedLatency() *schedLatency {
	s := &schedLatency{samples: []metrics.Sample{{Name: "/sched/latencies:seconds"}}}
	s.read()
	return s
}

// read returns the 99th percentile of the latencies since the previous read.
func (s *schedLatency) read() (float64, bool) {
	metrics.Read(s.samples)
	if s.samples[0].Value.Kind() != metrics.KindFloat64Histogram {
		return 0, false
	}

	h := s.samples[0].Value.Float64Histogram()
	prev := s.prev
	s.prev = slices.Clone(h.Counts)
	if len(prev) != len(h.Counts) {
		return 0, false
	}

	delta := make([]uint64, len(h.Counts))
	for i := range h.Counts {
		if h.Counts[i] > prev[i] {
			delta[i] = h.Counts[i] - prev[i]
		}
	}

	return percentile(h.Buckets, delta, 0.99), true
}

// percentile returns the upper bound of the bucket that holds the given
// percentile of a runtime histogram. buckets holds the bucket boundaries,
// one more than counts. It returns 0 for an empty histogram.
func percentile(buckets []float64, counts []uint64, p float64) float64 {
	var total uint64
	for _, c := range counts {
		total += c
	}
	if total == 0 || len(buckets) != len(counts)+1 {
		return 0
	}

	rank := uint64(math.Ceil(p * float64(total)))
	var cumulative uint64
	for i, c := range counts {
		cumulative += c
		if cumulative < rank {
			continue
		}
		if upper := buckets[i+1]; !math.IsInf(upper, 1) {
			return upper
		}
		return max(buckets[i], 0)
	}

	return 0
}
