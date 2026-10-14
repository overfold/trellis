package main

import (
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"syscall"
	"testing"
	"time"

	"github.com/overfold/trellis/orchestrator/api"
	"github.com/overfold/trellis/orchestrator/internal/execstream"
	"golang.org/x/sys/unix"
)

func TestExecSignalHelper(_ *testing.T) {
	address := os.Getenv("TRELLIS_EXEC_SIGNAL_TEST_SERVER")
	if address == "" {
		return
	}
	os.Args = []string{"trellisctl", "--namespace", "default", "--server-addr", address, "exec", "-it", "alloc-1", "--", "sh"}
	main()
}

func TestExecRestoresTerminalOnSignal(t *testing.T) {
	for _, sig := range []syscall.Signal{syscall.SIGTERM, syscall.SIGINT} {
		t.Run(sig.String(), func(t *testing.T) {
			master, err := os.OpenFile("/dev/ptmx", os.O_RDWR|syscall.O_NOCTTY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = master.Close() }()
			if err := unix.IoctlSetPointerInt(int(master.Fd()), unix.TIOCSPTLCK, 0); err != nil {
				t.Fatal(err)
			}
			number, err := unix.IoctlGetInt(int(master.Fd()), unix.TIOCGPTN)
			if err != nil {
				t.Fatal(err)
			}
			slave, err := os.OpenFile(fmt.Sprintf("/dev/pts/%d", number), os.O_RDWR|syscall.O_NOCTTY, 0)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = slave.Close() }()
			slaveFD := slave.Fd()
			before, err := terminalGetState(slaveFD)
			if err != nil {
				t.Fatal(err)
			}
			flagsBefore, err := unix.FcntlInt(slaveFD, unix.F_GETFL, 0)
			if err != nil {
				t.Fatal(err)
			}
			disconnected := make(chan struct{})
			server := newExecTestServer(t, func(api.ExecRequest) {}, func(reader *execstream.Reader, _ *execstream.Writer) {
				for {
					if _, err := reader.Next(); err != nil {
						close(disconnected)
						return
					}
				}
			})
			executable, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			process := exec.Command(executable, "-test.run=^TestExecSignalHelper$")
			process.Env = append(os.Environ(), "TRELLIS_EXEC_SIGNAL_TEST_SERVER="+server.URL, "TRELLIS_CONTEXT=", "HOME="+t.TempDir())
			log, err := os.CreateTemp(t.TempDir(), "exec-output")
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = log.Close() }()
			process.Stdin, process.Stdout, process.Stderr = slave, log, log
			if err := process.Start(); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = process.Process.Kill() })
			deadline := time.Now().Add(5 * time.Second)
			for {
				state, err := terminalGetState(slaveFD)
				if err != nil {
					t.Fatal(err)
				}
				if state.Lflag&unix.ICANON == 0 {
					break
				}
				if time.Now().After(deadline) {
					output, _ := os.ReadFile(log.Name())
					t.Fatalf("exec did not enter raw mode: %s", output)
				}
				time.Sleep(time.Millisecond)
			}
			if flagsDuring, err := unix.FcntlInt(slaveFD, unix.F_GETFL, 0); err != nil || flagsDuring != flagsBefore {
				t.Fatalf("exec changed caller's live descriptor flags: before=%d during=%d err=%v", flagsBefore, flagsDuring, err)
			}
			if err := process.Process.Signal(sig); err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() { done <- process.Wait() }()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				_ = process.Process.Signal(syscall.SIGQUIT)
				<-done
				output, _ := os.ReadFile(log.Name())
				t.Fatalf("exec did not exit on signal: %s", output)
			}
			after, err := terminalGetState(slaveFD)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("terminal not restored after %s: before=%+v after=%+v err=%v", sig, before, after, err)
			}
			if flagsAfter, err := unix.FcntlInt(slaveFD, unix.F_GETFL, 0); err != nil || flagsAfter != flagsBefore {
				t.Fatalf("stdin descriptor flags not restored: before=%d after=%d err=%v", flagsBefore, flagsAfter, err)
			}
			select {
			case <-disconnected:
			case <-time.After(time.Second):
				t.Fatal("signal did not close remote stream")
			}
		})
	}
}
