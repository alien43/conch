package core

import (
	"log/slog"
	"os"
)

// The stdout/stderr conch started with. A child normally gets os.Stdout and
// os.Stderr as they are when it starts (tests swap os.Stdout to capture its
// output). If one has become unusable, nil or closed, Fd() is -1, and Go starts
// the child with that fd closed: every write fails with EBADF, including the
// shell's own error message, so the child exits 1 without a word. A long-running
// conchd got into that state on two nodes in 2026-11 and its backup jobs failed
// silently for a night, while its own logging, built on these, kept working.
var startStdout, startStderr = os.Stdout, os.Stderr

// childStdio returns the stdout and stderr to hand a child, falling back to
// the ones conch started with (and saying so) when the current ones are unusable.
func childStdio(logger *slog.Logger, name string) (stdout, stderr *os.File) {
	return usableStdio(logger, name, "stdout", os.Stdout, startStdout),
		usableStdio(logger, name, "stderr", os.Stderr, startStderr)
}

func usableStdio(logger *slog.Logger, name, stream string, cur, start *os.File) *os.File {
	if cur != nil && cur.Fd() != ^uintptr(0) {
		return cur
	}
	// replaced tells the two ways apart: the os variable now points elsewhere,
	// or the object conch started with was itself closed.
	logger.Error("child-stdio-unusable", "name", name, "stream", stream,
		"nil", cur == nil, "replaced", cur != start, "fallback_fd", int(start.Fd()))
	return start
}
