package limit

import (
	"bufio"
	"fmt"
	"strconv"
	"strings"
)

// This file parses the formats of Linux's /proc and cgroup files. The
// parsers are free of build constraints so that their tests run everywhere.

// parseTable parses files like /proc/net/snmp and /proc/net/netstat: pairs
// of lines that start with the same prefix, the first with the field names,
// the second with the values. It returns the values by prefix and name.
func parseTable(content string) map[string]map[string]uint64 {
	out := map[string]map[string]uint64{}
	var names []string
	prefix := ""
	for line := range strings.Lines(content) {
		head, rest, ok := strings.Cut(strings.TrimSpace(line), ":")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		if head != prefix || names == nil {
			prefix, names = head, fields
			continue
		}

		values := map[string]uint64{}
		for i, f := range fields {
			if i >= len(names) {
				break
			}
			// some fields, e.g., Tcp MaxConn, are negative. They are no counters.
			if v, err := strconv.ParseUint(f, 10, 64); err == nil {
				values[names[i]] = v
			}
		}
		out[head] = values
		names = nil
	}
	return out
}

// parseKeyValues parses files with one name and one value per line, like
// /proc/net/snmp6, cpu.stat, and memory.stat.
func parseKeyValues(content string) map[string]uint64 {
	out := map[string]uint64{}
	for line := range strings.Lines(content) {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		if v, err := strconv.ParseUint(fields[1], 10, 64); err == nil {
			out[fields[0]] = v
		}
	}
	return out
}

// parseSockstat parses /proc/net/sockstat and /proc/net/sockstat6, for
// example "TCP: inuse 5 orphan 0 tw 2 alloc 6 mem 1".
func parseSockstat(content string) map[string]map[string]uint64 {
	out := map[string]map[string]uint64{}
	for line := range strings.Lines(content) {
		head, rest, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		fields := strings.Fields(rest)
		values := map[string]uint64{}
		for i := 0; i+1 < len(fields); i += 2 {
			if v, err := strconv.ParseUint(fields[i+1], 10, 64); err == nil {
				values[fields[i]] = v
			}
		}
		out[strings.TrimSpace(head)] = values
	}
	return out
}

// udpSocket is a line of /proc/net/udp or /proc/net/udp6.
type udpSocket struct {
	inode uint64

	// rxQueue is the memory that the datagrams in the receive buffer take,
	// in bytes.
	rxQueue uint64

	// drops is the number of datagrams that the socket dropped.
	drops uint64
}

// parseUDPSockets parses /proc/net/udp or /proc/net/udp6.
func parseUDPSockets(content string) ([]udpSocket, error) {
	var out []udpSocket
	scanner := bufio.NewScanner(strings.NewReader(content))
	header := true
	for scanner.Scan() {
		if header {
			header = false
			continue
		}

		// sl local rem st tx_queue:rx_queue tr:tm retrnsmt uid timeout inode ref pointer drops
		fields := strings.Fields(scanner.Text())
		if len(fields) < 13 {
			continue
		}

		_, rx, ok := strings.Cut(fields[4], ":")
		if !ok {
			continue
		}
		rxQueue, err := strconv.ParseUint(rx, 16, 64)
		if err != nil {
			continue
		}
		inode, err := strconv.ParseUint(fields[9], 10, 64)
		if err != nil {
			continue
		}
		drops, err := strconv.ParseUint(fields[12], 10, 64)
		if err != nil {
			continue
		}

		out = append(out, udpSocket{inode: inode, rxQueue: rxQueue, drops: drops})
	}
	return out, scanner.Err()
}

// parseStatusField returns a numeric field of /proc/self/status, for
// example "Threads".
func parseStatusField(content string, name string) (uint64, error) {
	for line := range strings.Lines(content) {
		key, value, ok := strings.Cut(line, ":")
		if !ok || key != name {
			continue
		}
		return strconv.ParseUint(strings.TrimSpace(value), 10, 64)
	}
	return 0, fmt.Errorf("no %s field", name)
}

// parseMeminfo returns the fields of /proc/meminfo in bytes.
func parseMeminfo(content string) map[string]uint64 {
	out := map[string]uint64{}
	for line := range strings.Lines(content) {
		key, value, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		fields := strings.Fields(value)
		if len(fields) == 0 {
			continue
		}
		v, err := strconv.ParseUint(fields[0], 10, 64)
		if err != nil {
			continue
		}
		if len(fields) > 1 && fields[1] == "kB" {
			v *= 1024
		}
		out[key] = v
	}
	return out
}

// parsePSI returns the avg10 share of the "some" line of a pressure stall
// information file, for example "some avg10=1.53 avg60=0.40 avg300=0.10
// total=1234", as a share between 0 and 1.
func parsePSI(content string) (float64, error) {
	for line := range strings.Lines(content) {
		fields := strings.Fields(line)
		if len(fields) < 2 || fields[0] != "some" {
			continue
		}
		for _, f := range fields[1:] {
			if v, ok := strings.CutPrefix(f, "avg10="); ok {
				pct, err := strconv.ParseFloat(v, 64)
				if err != nil {
					return 0, err
				}
				return pct / 100, nil
			}
		}
	}
	return 0, fmt.Errorf("no some avg10 value")
}

// parseUints parses the whitespace separated numbers of files like
// /proc/sys/net/ipv4/tcp_mem.
func parseUints(content string) ([]uint64, error) {
	fields := strings.Fields(content)
	out := make([]uint64, len(fields))
	for i, f := range fields {
		v, err := strconv.ParseUint(f, 10, 64)
		if err != nil {
			return nil, err
		}
		out[i] = v
	}
	return out, nil
}

// parseLimit parses a cgroup limit like memory.max or pids.max. "max" means
// no limit and returns false.
func parseLimit(content string) (uint64, bool, error) {
	s := strings.TrimSpace(content)
	if s == "max" {
		return 0, false, nil
	}
	v, err := strconv.ParseUint(s, 10, 64)
	if err != nil {
		return 0, false, err
	}
	return v, true, nil
}

// parseCPUMax parses cgroup v2's cpu.max, for example "200000 100000" or
// "max 100000", into the number of CPUs. It returns false without a quota.
func parseCPUMax(content string) (float64, bool, error) {
	fields := strings.Fields(content)
	if len(fields) != 2 {
		return 0, false, fmt.Errorf("unexpected cpu.max %q", content)
	}
	if fields[0] == "max" {
		return 0, false, nil
	}
	quota, err := strconv.ParseFloat(fields[0], 64)
	if err != nil {
		return 0, false, err
	}
	period, err := strconv.ParseFloat(fields[1], 64)
	if err != nil || period <= 0 {
		return 0, false, fmt.Errorf("unexpected cpu.max period %q", fields[1])
	}
	return quota / period, true, nil
}

// parseSocketLink returns the inode of a /proc/self/fd link to a socket,
// "socket:[12345]".
func parseSocketLink(link string) (uint64, bool) {
	s, ok := strings.CutPrefix(link, "socket:[")
	if !ok {
		return 0, false
	}
	s, ok = strings.CutSuffix(s, "]")
	if !ok {
		return 0, false
	}
	inode, err := strconv.ParseUint(s, 10, 64)
	return inode, err == nil
}
