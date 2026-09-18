//go:build darwin || dragonfly || freebsd || netbsd || openbsd

package main

import "golang.org/x/sys/unix"

func terminalGetState(fd uintptr) (*unix.Termios, error) {
	return unix.IoctlGetTermios(int(fd), unix.TIOCGETA)
}

func terminalSetState(fd uintptr, state *unix.Termios) error {
	return unix.IoctlSetTermios(int(fd), unix.TIOCSETA, state)
}
