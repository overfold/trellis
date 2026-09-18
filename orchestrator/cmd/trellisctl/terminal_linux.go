//go:build linux

package main

import "golang.org/x/sys/unix"

func terminalGetState(fd uintptr) (*unix.Termios, error) {
	return unix.IoctlGetTermios(int(fd), unix.TCGETS)
}

func terminalSetState(fd uintptr, state *unix.Termios) error {
	return unix.IoctlSetTermios(int(fd), unix.TCSETS, state)
}
