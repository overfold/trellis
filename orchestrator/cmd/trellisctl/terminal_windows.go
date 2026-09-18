//go:build windows

package main

import "golang.org/x/sys/windows"

func terminalIsTTY(fd uintptr) bool {
	var mode uint32
	return windows.GetConsoleMode(windows.Handle(fd), &mode) == nil
}

func terminalMakeRaw(inputFD, outputFD uintptr) (func() error, error) {
	inputHandle := windows.Handle(inputFD)
	var inputMode uint32
	if err := windows.GetConsoleMode(inputHandle, &inputMode); err != nil {
		return nil, err
	}

	rawInputMode := inputMode
	rawInputMode &^= windows.ENABLE_ECHO_INPUT | windows.ENABLE_LINE_INPUT | windows.ENABLE_PROCESSED_INPUT
	rawInputMode |= windows.ENABLE_VIRTUAL_TERMINAL_INPUT
	if err := windows.SetConsoleMode(inputHandle, rawInputMode); err != nil {
		return nil, err
	}

	var outputHandle windows.Handle
	var outputMode uint32
	outputChanged := false
	if outputFD != 0 {
		outputHandle = windows.Handle(outputFD)
		if err := windows.GetConsoleMode(outputHandle, &outputMode); err == nil {
			rawOutputMode := outputMode | windows.ENABLE_PROCESSED_OUTPUT | windows.ENABLE_VIRTUAL_TERMINAL_PROCESSING
			if err := windows.SetConsoleMode(outputHandle, rawOutputMode); err != nil {
				_ = windows.SetConsoleMode(inputHandle, inputMode)
				return nil, err
			}
			outputChanged = true
		}
	}

	return func() error {
		var restoreErr error
		if outputChanged {
			restoreErr = windows.SetConsoleMode(outputHandle, outputMode)
		}
		if err := windows.SetConsoleMode(inputHandle, inputMode); err != nil && restoreErr == nil {
			restoreErr = err
		}
		return restoreErr
	}, nil
}

func terminalSize(_, outputFD uintptr) (uint32, uint32, bool) {
	if outputFD == 0 {
		return 0, 0, false
	}
	var info windows.ConsoleScreenBufferInfo
	if err := windows.GetConsoleScreenBufferInfo(windows.Handle(outputFD), &info); err != nil {
		return 0, 0, false
	}
	cols := int32(info.Window.Right) - int32(info.Window.Left) + 1
	rows := int32(info.Window.Bottom) - int32(info.Window.Top) + 1
	if cols <= 0 || rows <= 0 {
		return 0, 0, false
	}
	return uint32(cols), uint32(rows), true
}
