//go:build !linux && !darwin && !dragonfly && !freebsd && !netbsd && !openbsd && !windows

package main

import "fmt"

func terminalIsTTY(uintptr) bool {
	return false
}

func terminalMakeRaw(uintptr, uintptr) (func() error, error) {
	return nil, fmt.Errorf("interactive terminal mode is not supported on this platform")
}

func terminalSize(uintptr, uintptr) (uint32, uint32, bool) {
	return 0, 0, false
}
