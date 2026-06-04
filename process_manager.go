package main

import (
	"os"
	"os/signal"
	"syscall"

	"github.com/rs/zerolog/log"
)

type Process struct {
	Process    *os.Process
	FinalState *os.ProcessState
}

func startProcess(chanProcess chan *Process) (*Process, error) {
	log.Info().Msg("Starting new process")

	procAttr := os.ProcAttr{
		Files: []*os.File{os.Stdin, os.Stdout, os.Stderr},
	}

	// Ensure the children does not have the --process-manager switch
	var args = make([]string, 0, len(os.Args))
	for _, v := range os.Args {
		if v == "--process-manager" || v == "-process-manager" {
			continue
		}
		args = append(args, v)
	}
	args = append(args, "--notify-parent")

	p := &Process{}

	proc, err := os.StartProcess(args[0], args, &procAttr)
	if err != nil {
		log.Error().Err(err).Msg("Error while starting new process")
		return p, err
	}
	p.Process = proc

	go func() {
		s, _ := proc.Wait()
		p.FinalState = s
		chanProcess <- p
	}()

	return p, err
}

func processManager() {
	chanSignals := make(chan os.Signal, 1)
	signal.Notify(chanSignals, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP, syscall.SIGUSR1)

	processManagerLoop(chanSignals)
}

func processManagerLoop(chanSignals <-chan os.Signal) {
	processes := map[*Process]*Process{}
	chanProcess := make(chan *Process)
	var starting *Process

	// Start first worker process
	proc, err := startProcess(chanProcess)
	if err != nil {
		log.Panic().Err(err).Msg("Unable to start the worker process")
	}
	processes[proc] = proc
	starting = proc

loop:
	for {
		select {
		case s := <-chanSignals:
			switch s {
			case syscall.SIGINT, syscall.SIGTERM:
				log.Info().Msg("Termination signal received, forwarding to worker processes")
				for _, p := range processes {
					p.Process.Signal(syscall.SIGTERM)
				}

			case syscall.SIGHUP:
				if starting != nil {
					log.Warn().Msg("Restart signal received but a restart is already ongoing")

				} else {
					log.Info().Msg("Restart signal received, starting new worker process")
					proc, err := startProcess(chanProcess)
					if err != nil {
						log.Error().Err(err).Msg("Unable to start the new worker process, keeping current running to avoid disruption.")
						starting = nil
					} else {
						processes[proc] = proc
						starting = proc
					}
				}

			case syscall.SIGUSR1:
				log.Info().Msg("New worker successfully started")
				for _, p := range processes {
					if p != starting {
						p.Process.Signal(syscall.SIGTERM)
					}
				}
				starting = nil
			}
		case p := <-chanProcess:
			if p == starting {
				log.Error().Int("workerPid", p.Process.Pid).Int("exitCode", p.FinalState.ExitCode()).Msg("New worker process exited unexpectedly")
				starting = nil
			} else {
				log.Info().Int("workerPid", p.Process.Pid).Int("exitCode", p.FinalState.ExitCode()).Msg("Worker process exited")
			}

			delete(processes, p)

			if len(processes) == 0 {
				log.Info().Msg("All worker processes have ended")
				break loop
			}
		}
	}
}
