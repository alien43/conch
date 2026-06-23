package core

import (
	"math/rand"
	"time"
)

type Backoff struct {
	Base    time.Duration
	Cap     time.Duration
	current time.Duration
}

func NewBackoff(base, cap time.Duration) *Backoff {
	return &Backoff{
		Base:    base,
		Cap:     cap,
		current: base,
	}
}

func (b *Backoff) Duration() time.Duration {
	d := b.current
	maxVal := int64(d * 3)
	minVal := int64(b.Base)
	var jittered int64
	if maxVal > minVal {
		jittered = minVal + rand.Int63n(maxVal-minVal)
	} else {
		jittered = minVal
	}
	next := time.Duration(jittered)
	if next > b.Cap {
		next = b.Cap
	}
	b.current = next
	return d
}

func (b *Backoff) Reset() {
	b.current = b.Base
}
