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
		if strings.HasPrefix(line, "read_bytes:") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				return 0, fmt.Errorf("invalid read_bytes line: %s", line)
			}
			return strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("read_bytes not found")
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
		if strings.HasPrefix(line, "write_bytes:") {
			fields := strings.Fields(line)
			if len(fields) != 2 {
				return 0, fmt.Errorf("invalid write_bytes line: %s", line)
			}
			return strconv.ParseUint(fields[1], 10, 64)
		}
	}
	return 0, fmt.Errorf("write_bytes not found")
}
