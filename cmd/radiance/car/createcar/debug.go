package createcar

import (
	"fmt"
	mathrand "math/rand"
	"strings"

	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/printer"
)

func ______SEPARATOR______() {
	fmt.Printf("\x1b[31m%s\x1b[0m	\n", strings.Repeat("=", 80))
}

var debugMode bool = false

func debugPrintNode(node datamodel.Node) {
	if debugMode {
		printer.Print(node)
	}
}

func newTestSlots(numSlots int) []uint64 {
	slots := make([]uint64, numSlots)
	for i := 0; i < numSlots; i++ {
		slots[i] = uint64(i)
	}
	mathrand.Shuffle(len(slots), func(i, j int) {
		slots[i], slots[j] = slots[j], slots[i]
	})
	return slots
}
