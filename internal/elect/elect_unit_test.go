package elect

import (
	"bytes"
	"io"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/alien43/conch/internal/core"
)

func TestBackoffSequence(t *testing.T) {
	b := core.NewBackoff(1*time.Second, 30*time.Second)

	// First duration should be the base duration
	d1 := b.Duration()
	if d1 != 1*time.Second {
		t.Errorf("expected 1s, got %v", d1)
	}

	// Subsequent durations should increase (exponentially with jitter)
	d2 := b.Duration()
	if d2 < 1*time.Second {
		t.Errorf("expected duration to be at least base 1s, got %v", d2)
	}

	// Reset should bring it back to the base duration
	b.Reset()
	d3 := b.Duration()
	if d3 != 1*time.Second {
		t.Errorf("expected 1s after reset, got %v", d3)
	}
}

func captureStdout(f func()) string {
	r, w, _ := os.Pipe()
	oldStdout := os.Stdout
	os.Stdout = w

	defer func() {
		os.Stdout = oldStdout
	}()

	f()

	w.Close()
	var buf bytes.Buffer
	_, _ = io.Copy(&buf, r)
	return buf.String()
}

func TestPrintLeader(t *testing.T) {
	// Case 1: c == nil, useJSON = false
	out1 := captureStdout(func() {
		printLeader("office-test", nil, false)
	})
	if out1 != "office-test -\n" {
		t.Errorf("unexpected output: %q", out1)
	}

	// Case 2: c == nil, useJSON = true
	out2 := captureStdout(func() {
		printLeader("office-test", nil, true)
	})
	if out2 != "{\"office\":\"office-test\",\"leader\":null}\n" {
		t.Errorf("unexpected output: %q", out2)
	}

	// Case 3: c != nil, useJSON = true
	valJSON := `{"host":"node1","pid":1234,"started":"2026-06-23T12:00:00Z"}`
	c1 := &candidate{
		Key:            "some-key",
		CreateRevision: 100,
		Value:          []byte(valJSON),
	}
	out3 := captureStdout(func() {
		printLeader("office-test", c1, true)
	})
	if out3 != valJSON+"\n" {
		t.Errorf("unexpected output: %q", out3)
	}

	// Case 4: c != nil, useJSON = false
	out4 := captureStdout(func() {
		printLeader("office-test", c1, false)
	})
	if !strings.Contains(out4, "office-test node1 pid=1234") {
		t.Errorf("unexpected output: %q", out4)
	}
}
