package iostats

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// GetDiskReadBytes returns how much this process has read from disk.
func GetDiskReadBytes() (uint64, error) {
	file, err := os.Open("/proc/self/io")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "rchar:") {
			v, err := getField1AsUint64(line)
			if err != nil {
				return 0, err
			}
			return v, nil
		}
	}
	return 0, fmt.Errorf("rchar not found")
}

func getField1AsUint64(line string) (uint64, error) {
	fields := strings.Fields(line)
	if len(fields) != 2 {
		return 0, fmt.Errorf("invalid line: %s", line)
	}
	return strconv.ParseUint(fields[1], 10, 64)
}

// GetDiskWriteBytes returns how much this process has written to disk.
func GetDiskWriteBytes() (uint64, error) {
	file, err := os.Open("/proc/self/io")
	if err != nil {
		return 0, err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "wchar:") {
			v, err := getField1AsUint64(line)
			if err != nil {
				return 0, err
			}
			return v, nil
		}
	}
	return 0, fmt.Errorf("wchar not found")
}
