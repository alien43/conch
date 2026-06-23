package core

import (
	"testing"
	"time"
)

func TestBackoff(t *testing.T) {
	base := 1 * time.Second
	cap := 10 * time.Second
	b := NewBackoff(base, cap)

	if b.Base != base {
		t.Errorf("expected Base %v, got %v", base, b.Base)
	}
	if b.Cap != cap {
		t.Errorf("expected Cap %v, got %v", cap, b.Cap)
	}

	// First duration returned should be the base duration
	d1 := b.Duration()
	if d1 != base {
		t.Errorf("expected first duration %v, got %v", base, d1)
	}

	// Subsequent duration calls should update b.current, bounded by cap
	for i := 0; i < 10; i++ {
		d := b.Duration()
		if d > cap {
			t.Errorf("duration %v exceeded cap %v", d, cap)
		}
	}

	// Reset should reset current backoff
	b.Reset()
	d2 := b.Duration()
	if d2 != base {
		t.Errorf("expected reset duration %v, got %v", base, d2)
	}
}
