package blockstore

import (
	"fmt"

	"github.com/rpcpool/yellowstone-faithful/blockmarker"
)

// ValidateMarkerLayout checks a slot's markers against the layout Agave enforces
// (runtime/src/block_component_processor.rs):
//
//	Header, [GenesisCertificate], batches, [UpdateParent, batches], Footer, tick batch
//
// entries is the output of DataShredsToEntries for a slot with at least one marker.
func ValidateMarkerLayout(entries []Entries) error {
	var (
		counts      [blockmarker.VariantGenesisCertificate + 1]int
		footerSeen  bool
		afterFooter int
	)
	for i, e := range entries {
		if footerSeen {
			if e.Marker != nil {
				return fmt.Errorf("%w: %s after the footer", blockmarker.ErrInvalid, e.Marker.Variant)
			}
			afterFooter++
			if afterFooter > 1 {
				return fmt.Errorf("%w: %d entry batches after the footer", blockmarker.ErrInvalid, afterFooter)
			}
			for _, entry := range e.Entries {
				if len(entry.Txns) > 0 {
					return fmt.Errorf("%w: transactions after the footer", blockmarker.ErrInvalid)
				}
			}
			continue
		}
		if e.Marker == nil {
			continue
		}
		v := e.Marker.Variant
		counts[v]++
		switch v {
		case blockmarker.VariantHeader:
			if i != 0 {
				return fmt.Errorf("%w: header is not the first block component", blockmarker.ErrInvalid)
			}
		case blockmarker.VariantGenesisCertificate:
			if i != 1 || entries[0].Marker == nil || entries[0].Marker.Variant != blockmarker.VariantHeader {
				return fmt.Errorf("%w: genesis certificate is not directly after the header", blockmarker.ErrInvalid)
			}
		case blockmarker.VariantFooter:
			footerSeen = true
		}
	}
	for _, v := range []blockmarker.Variant{blockmarker.VariantHeader, blockmarker.VariantFooter} {
		if counts[v] != 1 {
			return fmt.Errorf("%w: %d %s markers, want 1", blockmarker.ErrInvalid, counts[v], v)
		}
	}
	for _, v := range []blockmarker.Variant{blockmarker.VariantUpdateParent, blockmarker.VariantGenesisCertificate} {
		if counts[v] > 1 {
			return fmt.Errorf("%w: %d %s markers, want at most 1", blockmarker.ErrInvalid, counts[v], v)
		}
	}
	return nil
}
