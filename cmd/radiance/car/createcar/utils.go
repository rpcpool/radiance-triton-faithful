package createcar

import (
	"bytes"
	"fmt"
	"os"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/ipld/go-ipld-prime/datamodel"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
)

type CountWriter struct {
	Count int
}

func (w *CountWriter) Write(p []byte) (n int, err error) {
	w.Count += len(p)
	return len(p), nil
}

func SizeOfFile(path string) (int, error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	defer file.Close()

	fi, err := file.Stat()
	if err != nil {
		return 0, err
	}
	return int(fi.Size()), nil
}

const (
	KiB = 1024
	MiB = 1024 * KiB
	GiB = 1024 * MiB
	TiB = 1024 * GiB
)

func fileExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if os.IsNotExist(err) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if info.IsDir() {
		return false, fmt.Errorf("path %s is a directory", path)
	}
	return true, nil
}

func SortLinksByCID(links []datamodel.Link) {
	sort.Slice(links, func(i, j int) bool {
		return bytes.Compare(links[i].(cidlink.Link).Cid.Bytes(), links[j].(cidlink.Link).Cid.Bytes()) < 0
	})
}

// ParseIntervals parses a string representing a list of unsigned integers or integer ranges (using dashes).
func ParseIntervals(s string) ([]int, error) {
	// trim space:
	s = strings.TrimSpace(s)
	// remove all spaces:
	s = strings.ReplaceAll(s, " ", "")
	// split by comma:
	commaSeparated := strings.Split(s, ",")

	var results []int
	for _, rr := range commaSeparated {
		dashCount := strings.Count(rr, "-")
		switch dashCount {
		case 0:
			{
				parsed, err := strconv.Atoi(rr)
				if err != nil {
					return nil, fmt.Errorf("error parsing %q: %s", rr, err)
				}
				results = append(results, parsed)
			}
		case 1:
			{
				start, end, err := parseRange(rr)
				if err != nil {
					return nil, err
				}
				rangeValues := GenerateIntRangeInclusive(start, end)
				results = append(results, rangeValues...)
			}
		default:
			{
				return nil, fmt.Errorf("error: %q contains too many dashes", rr)
			}
		}
	}

	return results, nil
}

func parseRange(s string) (int, int, error) {
	rangeVals := strings.Split(s, "-")
	if len(rangeVals) != 2 {
		return 0, 0, fmt.Errorf("cannot parse range: %q", s)
	}

	rangeStartString := rangeVals[0]
	start, err := strconv.Atoi(rangeStartString)
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing range start %q: %s", rangeStartString, err)
	}

	rangeEndString := rangeVals[1]
	end, err := strconv.Atoi(rangeEndString)
	if err != nil {
		return 0, 0, fmt.Errorf("error parsing range end %q: %s", rangeEndString, err)
	}
	return start, end, nil
}

func example() {
	res, err := ParseIntervals("99,5,1-3,98-90")
	if err != nil {
		panic(err)
	}

	fmt.Println(res)
	fmt.Println(DeduplicateInts(res))
}

func DeduplicateInts(ints []int) []int {
	sort.Ints(ints)
	return slices.Compact(ints)
}

func GenerateIntRangeInclusive(from, to int) []int {
	ints := []int{}
	if from <= to {
		for i := from; i <= to; i++ {
			ints = append(ints, i)
		}
	} else {
		for i := from; i >= to; i-- {
			ints = append(ints, i)
		}
	}
	return ints
}
