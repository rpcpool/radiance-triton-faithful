package createcar

import (
	"sort"

	"github.com/ipld/go-ipld-prime/datamodel"
)

type SlotToLink map[uint64]datamodel.Link

func (s2l SlotToLink) GetLinksSortedBySlot() []datamodel.Link {
	slots := make([]uint64, 0)
	for slot := range s2l {
		slots = append(slots, slot)
	}
	sort.Slice(slots, func(i, j int) bool {
		return slots[i] < slots[j]
	})
	links := make([]datamodel.Link, len(slots))
	for i, slot := range slots {
		links[i] = s2l[slot]
	}
	return links
}
