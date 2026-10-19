package maddr

import (
	"context"
	"fmt"

	ma "github.com/multiformats/go-multiaddr"
	madns "github.com/multiformats/go-multiaddr-dns"
)

// IsDNSAddr reports whether m starts with a /dnsaddr component.
func IsDNSAddr(m ma.Multiaddr) bool {
	first, _ := ma.SplitFirst(m)
	return first != nil && first.Code() == ma.P_DNSADDR
}

// ResolveDNSAddr looks up the TXT records of a multiaddress that starts with
// /dnsaddr, for example a bootstrap entry without a peer ID such as
// /dnsaddr/bootstrap-mainnet.chainsafe-fil.io. It returns the listed
// addresses that end in a /p2p/ component and drops the others.
//
// It resolves one level only, as Lotus does for its bootstrap lists. A
// listed /dnsaddr address that ends in /p2p/ is returned as it is; a libp2p
// host resolves it when it dials. madns returns at most 100 addresses.
//
// It returns an error if m does not start with /dnsaddr, if the lookup
// fails, or if no listed address ends in /p2p/.
func ResolveDNSAddr(ctx context.Context, r *madns.Resolver, m ma.Multiaddr) ([]ma.Multiaddr, error) {
	if !IsDNSAddr(m) {
		return nil, fmt.Errorf("resolve %s: not a /dnsaddr address", m)
	}

	resolved, err := r.Resolve(ctx, m)
	if err != nil {
		return nil, fmt.Errorf("resolve %s: %w", m, err)
	}

	var out []ma.Multiaddr
	for _, rm := range resolved {
		if _, last := ma.SplitLast(rm); last != nil && last.Code() == ma.P_P2P {
			out = append(out, rm)
		}
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("resolve %s: no listed address ends in /p2p/", m)
	}

	return out, nil
}
