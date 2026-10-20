//go:build linux

package limit

import (
	"net"

	"github.com/safchain/ethtool"
)

// enaDriver is the driver name that ethtool reports for the AWS Elastic
// Network Adapter.
const enaDriver = "ena"

// enaReader returns a function that sums the ethtool statistics of the ENA
// interfaces of the machine, or nil if it has none. The ethtool ioctls need
// no capabilities. The interfaces are looked up once.
func enaReader() func() (map[string]uint64, bool) {
	e, err := ethtool.NewEthtool()
	if err != nil {
		return nil
	}
	defer e.Close()

	ifaces, err := net.Interfaces()
	if err != nil {
		return nil
	}

	var names []string
	for _, iface := range ifaces {
		if iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		if driver, err := e.DriverName(iface.Name); err == nil && driver == enaDriver {
			names = append(names, iface.Name)
		}
	}
	if len(names) == 0 {
		return nil
	}

	return func() (map[string]uint64, bool) {
		e, err := ethtool.NewEthtool()
		if err != nil {
			return nil, false
		}
		defer e.Close()

		sum := map[string]uint64{}
		ok := false
		for _, name := range names {
			stats, err := e.Stats(name)
			if err != nil {
				continue
			}
			ok = true
			for k, v := range stats {
				sum[k] += v
			}
		}
		return sum, ok
	}
}
