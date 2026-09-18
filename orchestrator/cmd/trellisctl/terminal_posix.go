//go:build linux || darwin || dragonfly || freebsd || netbsd || openbsd

package main

import "golang.org/x/sys/unix"

func terminalIsTTY(fd uintptr) bool {
	_, err := terminalGetState(fd)
	return err == nil
}

func terminalMakeRaw(inputFD, _ uintptr) (func() error, error) {
	state, err := terminalGetState(inputFD)
	if err != nil {
		return nil, err
	}
	raw := *state
	raw.Iflag &^= unix.BRKINT | unix.ICRNL | unix.INPCK | unix.ISTRIP | unix.IXON
	raw.Oflag &^= unix.OPOST
	raw.Cflag &^= unix.CSIZE | unix.PARENB
	raw.Cflag |= unix.CS8
	raw.Lflag &^= unix.ECHO | unix.ICANON | unix.IEXTEN | unix.ISIG
	raw.Cc[unix.VMIN] = 1
	raw.Cc[unix.VTIME] = 0

	if err := terminalSetState(inputFD, &raw); err != nil {
		return nil, err
	}
	return func() error {
		return terminalSetState(inputFD, state)
	}, nil
}

func terminalSize(inputFD, _ uintptr) (uint32, uint32, bool) {
	size, err := unix.IoctlGetWinsize(int(inputFD), unix.TIOCGWINSZ)
	if err != nil || size.Col == 0 || size.Row == 0 {
		return 0, 0, false
	}
	return uint32(size.Col), uint32(size.Row), true
}
