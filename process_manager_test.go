package main

import (
	"flag"
	"os"
	"syscall"
	"testing"
	"time"
)

func init() {
	// Ensure the notify-parent flag is registered during tests to avoid flag parsing errors.
	if flag.Lookup("notify-parent") == nil {
		flag.Bool("notify-parent", false, "dummy flag for testing")
	}
}

// TestHelperProcess is a helper function that acts as a mock worker process.
// It is invoked by spawning the test executable itself with specific environment variables.
// It handles different actions: success exit, error exit, and long sleep.
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

// TestProcessManager verifies the main entry point of the process manager.
// It simulates a scenario where the worker process exits with an error code,
// ensuring the process manager correctly detects the exit and terminates.
func TestProcessManager(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	// Pass --process-manager and -process-manager to ensure they are correctly filtered out by startProcess
	os.Args = []string{os.Args[0], "--process-manager", "-process-manager", "-test.run=TestHelperProcess"}

	os.Setenv("GO_WANT_HELPER_PROCESS", "1")
	os.Setenv("GO_HELPER_ACTION", "exit_error") // Simulate a worker that fails
	defer os.Unsetenv("GO_WANT_HELPER_PROCESS")
	defer os.Unsetenv("GO_HELPER_ACTION")

	// Call processManager directly. It should return as soon as the worker process exits.
	processManager()
}

// TestProcessManagerLoop_Signals verifies the process manager's signal handling logic.
// It tests:
// 1. SIGHUP: Triggering a process restart (and handling the 'already starting' state).
// 2. SIGUSR1: Clearing the 'starting' state.
// 3. SIGINT/SIGTERM: Graceful termination of all managed worker processes.
func TestProcessManagerLoop_Signals(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{os.Args[0], "-test.run=TestHelperProcess"}

	os.Setenv("GO_WANT_HELPER_PROCESS", "1")
	os.Setenv("GO_HELPER_ACTION", "sleep") // Ensure mock workers don't exit prematurely
	defer os.Unsetenv("GO_WANT_HELPER_PROCESS")
	defer os.Unsetenv("GO_HELPER_ACTION")

	chanSignals := make(chan os.Signal, 10)

	go func() {
		// Wait for the first worker process to be started by the loop
		time.Sleep(200 * time.Millisecond)

		// 1. Send SIGHUP while 'starting' state is active. Should be ignored as "already ongoing".
		chanSignals <- syscall.SIGHUP
		time.Sleep(100 * time.Millisecond)

		// 2. Send SIGUSR1 to manually clear the 'starting' state.
		chanSignals <- syscall.SIGUSR1
		time.Sleep(100 * time.Millisecond)

		// 3. Send SIGHUP while 'starting' is nil. Should trigger a new worker process start.
		chanSignals <- syscall.SIGHUP
		time.Sleep(200 * time.Millisecond)

		// 4. Send SIGUSR1 again to clear state.
		chanSignals <- syscall.SIGUSR1
		time.Sleep(100 * time.Millisecond)

		// 5. Send SIGINT to signal overall shutdown and termination of all workers.
		chanSignals <- syscall.SIGINT
	}()

	processManagerLoop(chanSignals)
}

// TestProcessManagerLoop_StartProcessError verifies that the process manager loop
// continues to function correctly even if a request to start a new process fails.
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
		// Wait for the first worker to start
		time.Sleep(200 * time.Millisecond)

		// Clear the 'starting' state
		chanSignals <- syscall.SIGUSR1
		time.Sleep(100 * time.Millisecond)

		// Corrupt os.Args to ensure the next startProcess call fails (executable not found)
		os.Args = []string{"/nonexistent/path/to/executable"}

		// Trigger SIGHUP, which should encounter the start error and handle it gracefully
		chanSignals <- syscall.SIGHUP
		time.Sleep(100 * time.Millisecond)

		// Terminate the manager loop
		chanSignals <- syscall.SIGTERM
	}()

	processManagerLoop(chanSignals)
}

// TestProcessManager_FirstStartPanic verifies that if the initial process start fails,
// the process manager correctly panics, as it cannot proceed without an initial worker.
func TestProcessManager_FirstStartPanic(t *testing.T) {
	oldArgs := os.Args
	defer func() { os.Args = oldArgs }()
	os.Args = []string{"/nonexistent/executable/path"} // Cause initial startProcess to fail

	defer func() {
		// Verify that a panic occurred as expected
		if r := recover(); r == nil {
			t.Errorf("expected processManager to panic on initial start failure")
		}
	}()

	processManager()
}
