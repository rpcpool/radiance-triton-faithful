package versioninfo

import (
	"runtime/debug"
)

func GetBuildSettings() ([]debug.BuildSetting, bool) {
	filtered := []debug.BuildSetting{}
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return filtered, false
	}

	for _, setting := range info.Settings {
		if isAnyOf(setting.Key,
			"-compiler",
			"GOARCH",
			"GOOS",
			"GOAMD64",
			"vcs",
			"vcs.revision",
			"vcs.time",
			"vcs.modified",
		) {
			filtered = append(filtered, setting)
		}
	}
	filtered = append(filtered, debug.BuildSetting{
		Key:   "goversion",
		Value: info.GoVersion,
	})
	return filtered, true
}

func isAnyOf(s string, anyOf ...string) bool {
	for _, v := range anyOf {
		if s == v {
			return true
		}
	}
	return false
}
