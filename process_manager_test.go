package main

import (
	"flag"
	"os"
	"syscall"
	"testing"
	"time"
)

func init() {
	if flag.Lookup("notify-parent") == nil {
		flag.Bool("notify-parent", false, "dummy")
	}
}

// TestHelperProcess acts as a mock worker process when spawned by startProcess.
func TestHelperProcess(t *testing.T) {
	if os.Getenv("GO_WANT_HELPER_PROCESS") != "1" {
		return
	}

	action := os.Getenv("GO_HELPER_ACTION")
	switch action {
	case "exit_success":
		os.Exit(0)
	case "exit_error":
		os.Exit(1)
	case "sleep":
		time.Sleep(10 * time.Second)
		os.Exit(0)
	}
	os.Exit(0)
}

func TestProcessManager(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	// Pass --process-manager and -process-manager to ensure they get filtered by startProcess
	os.Args = []string{os.Args[0], "--process-manager", "-process-manager", "-test.run=TestHelperProcess"}

	os.Setenv("GO_WANT_HELPER_PROCESS", "1")
	os.Setenv("GO_HELPER_ACTION", "exit_error") // Cover error exit code branch
	defer os.Unsetenv("GO_WANT_HELPER_PROCESS")
	defer os.Unsetenv("GO_HELPER_ACTION")

	// Call processManager directly. It should return as soon as the worker exits naturally.
	processManager()
}

func TestProcessManagerLoop_Signals(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{os.Args[0], "-test.run=TestHelperProcess"}

	os.Setenv("GO_WANT_HELPER_PROCESS", "1")
	os.Setenv("GO_HELPER_ACTION", "sleep") // Helper processes will block until killed
	defer os.Unsetenv("GO_WANT_HELPER_PROCESS")
	defer os.Unsetenv("GO_HELPER_ACTION")

	chanSignals := make(chan os.Signal, 10)

	go func() {
		// Wait for first worker to start
		time.Sleep(200 * time.Millisecond)

		// 1. SIGHUP while starting != nil -> "already ongoing"
		chanSignals <- syscall.SIGHUP
		time.Sleep(100 * time.Millisecond)

		// 2. SIGUSR1 -> sets starting = nil
		chanSignals <- syscall.SIGUSR1
		time.Sleep(100 * time.Millisecond)

		// 3. SIGHUP while starting == nil -> starts a new worker
		chanSignals <- syscall.SIGHUP
		time.Sleep(200 * time.Millisecond)

		// 4. SIGUSR1 -> kills worker1, sets starting = nil
		chanSignals <- syscall.SIGUSR1
		time.Sleep(100 * time.Millisecond)

		// 5. SIGINT -> forwards SIGTERM to all workers (worker2 is remaining)
		chanSignals <- syscall.SIGINT
	}()

	processManagerLoop(chanSignals)
}

func TestProcessManagerLoop_StartProcessError(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{os.Args[0], "-test.run=TestHelperProcess"}

	os.Setenv("GO_WANT_HELPER_PROCESS", "1")
	os.Setenv("GO_HELPER_ACTION", "sleep")
	defer os.Unsetenv("GO_WANT_HELPER_PROCESS")
	defer os.Unsetenv("GO_HELPER_ACTION")

	chanSignals := make(chan os.Signal, 10)

	go func() {
		// Wait for first worker to start
		time.Sleep(200 * time.Millisecond)

		// Set starting = nil via SIGUSR1
		chanSignals <- syscall.SIGUSR1
		time.Sleep(100 * time.Millisecond)

		// Change os.Args so the next startProcess fails
		os.Args = []string{"/does/not/exist/executable"}

		// Trigger SIGHUP, which will fail to start the process
		chanSignals <- syscall.SIGHUP
		time.Sleep(100 * time.Millisecond)

		// End the test by terminating remaining workers
		chanSignals <- syscall.SIGTERM
	}()

	processManagerLoop(chanSignals)
}

func TestProcessManager_FirstStartPanic(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"/does/not/exist"} // Cause startProcess to fail entirely

	defer func() {
		if r := recover(); r == nil {
			t.Errorf("expected panic")
		}
	}()

	processManager()
}
