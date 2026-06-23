package core

import (
	"encoding/json"
	"errors"
	"os"
	"os/exec"
	"testing"
)

func TestNewHolderJSON(t *testing.T) {
	cmdArgs := []string{"my-command", "arg1", "arg2"}
	bytes, err := NewHolderJSON(cmdArgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	var h HolderJSON
	if err := json.Unmarshal(bytes, &h); err != nil {
		t.Fatalf("failed to unmarshal JSON: %v", err)
	}

	hostname, _ := os.Hostname()
	if h.Host != hostname && h.Host != "unknown" {
		t.Errorf("expected Host %q, got %q", hostname, h.Host)
	}

	if h.Pid != os.Getpid() {
		t.Errorf("expected Pid %d, got %d", os.Getpid(), h.Pid)
	}

	if h.Cmd != "my-command arg1 arg2" {
		t.Errorf("expected Cmd %q, got %q", "my-command arg1 arg2", h.Cmd)
	}

	if h.Started == "" {
		t.Error("expected Started time, got empty string")
	}

	// Test truncation
	longArgs := make([]string, 300)
	for i := range longArgs {
		longArgs[i] = "a"
	}
	bytesLong, err := NewHolderJSON(longArgs)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	var hLong HolderJSON
	_ = json.Unmarshal(bytesLong, &hLong)
	if len(hLong.Cmd) != 256 {
		t.Errorf("expected Cmd length 256, got %d", len(hLong.Cmd))
	}
}

func TestGetExitCode(t *testing.T) {
	// Nil error -> exit 0
	if code := getExitCode(nil); code != 0 {
		t.Errorf("expected exit code 0, got %d", code)
	}

	// Generic error -> exit 1
	errGen := errors.New("generic error")
	if code := getExitCode(errGen); code != 1 {
		t.Errorf("expected exit code 1, got %d", code)
	}

	// ExitError with exit code (using a simulated cmd failure)
	cmd := exec.Command("sh", "-c", "exit 42")
	errExit := cmd.Run()
	if errExit == nil {
		t.Fatal("expected command to fail")
	}
	if code := getExitCode(errExit); code != 42 {
		t.Errorf("expected exit code 42, got %d", code)
	}

	// Signaled exit
	cmdSig := exec.Command("sleep", "10")
	_ = cmdSig.Start()
	_ = cmdSig.Process.Signal(os.Interrupt)
	errSig := cmdSig.Wait()
	if errSig == nil {
		t.Fatal("expected command to be interrupted")
	}
	// exit status should be 128 + SIGINT (2) = 130
	if code := getExitCode(errSig); code != 130 && code != 128+2 {
		t.Errorf("expected exit code 130, got %d", code)
	}
}
