package elect

import (
	"testing"
	"time"

	"conch/internal/core"
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
