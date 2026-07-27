// Package livelog holds the output of jobs that are still running, so you
// can watch one work instead of waiting for it to finish and hoping the
// answer is in the final blob.
//
// This is the one piece of control plane state that is deliberately NOT
// durable. Two reasons. Putting it in the WAL would mean an fsync per
// chunk of a chatty job's stdout, which is exactly the write pattern the
// WAL is worst at and would slow down the job submissions that actually
// need durability. And it would be redundant: a job's complete output is
// written to the WAL once, in the completion report, which is the copy
// that has to survive. Losing the live tail to a control plane restart
// costs you a progress bar, not a record.
//
// Memory is bounded by construction: each running job keeps at most
// perJobCap bytes, and a job's buffer is dropped the moment it finishes.
// The high-water mark is therefore roughly (jobs running at once) x cap,
// and the number of jobs running at once is already bounded by total
// worker capacity.
package livelog

import "sync"

// Log stores the recent output of running jobs, keyed by job ID.
// The zero value is not usable; call New.
type Log struct {
	mu        sync.Mutex
	bufs      map[string]*buffer
	perJobCap int
}

type buffer struct {
	data []byte
	// start is the absolute offset of data[0] in the job's whole output
	// stream. It only moves when old bytes are dropped to stay under the
	// cap, and it is what lets a reader tell "I am behind" from "I am
	// caught up" after a truncation.
	start int64
}

// New returns a Log that keeps at most perJobCap bytes per running job.
func New(perJobCap int) *Log {
	if perJobCap <= 0 {
		perJobCap = 256 * 1024
	}
	return &Log{
		bufs:      make(map[string]*buffer),
		perJobCap: perJobCap,
	}
}

// Append adds newly-produced output for a job.
//
// When a job produces more than the cap, the oldest bytes are dropped
// rather than the newest. A job that has been screaming into stdout for
// ten minutes is almost always being read for what it is doing *now*, and
// the complete output is still recorded in full when the job finishes.
func (l *Log) Append(jobID string, data []byte) {
	if len(data) == 0 {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()

	b := l.bufs[jobID]
	if b == nil {
		b = &buffer{}
		l.bufs[jobID] = b
	}
	b.data = append(b.data, data...)
	if overflow := len(b.data) - l.perJobCap; overflow > 0 {
		b.data = b.data[overflow:]
		b.start += int64(overflow)
	}
}

// Read returns buffered output at or after offset.
//
// from is the absolute offset of the first returned byte and next is the
// offset to ask for on the following call. truncated reports that the
// requested offset had already been dropped, so the caller knows there is
// a hole rather than silently reading past one. ok is false when nothing
// is buffered for this job at all.
func (l *Log) Read(jobID string, offset int64) (data []byte, from, next int64, truncated, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()

	b := l.bufs[jobID]
	if b == nil {
		return nil, 0, 0, false, false
	}
	end := b.start + int64(len(b.data))

	if offset < b.start {
		// Caller fell behind the window; give them everything still held
		// and tell them what they missed.
		out := make([]byte, len(b.data))
		copy(out, b.data)
		return out, b.start, end, true, true
	}
	if offset >= end {
		return nil, offset, end, false, true // caught up, nothing new
	}
	idx := offset - b.start
	out := make([]byte, len(b.data)-int(idx))
	copy(out, b.data[idx:])
	return out, offset, end, false, true
}

// Drop releases a job's buffer. Called when a job reaches a terminal
// state, at which point its full output lives in the job record and
// keeping a second copy in memory would be a slow leak.
func (l *Log) Drop(jobID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	delete(l.bufs, jobID)
}

// RetainOnly drops every buffer whose job ID is not in keep.
//
// Dropping on completion handles the ordinary path, but a job can also
// stop running because the reaper decided its worker died, and that path
// never touches this package. Rather than chase every exit, the janitor
// periodically reconciles against the set of jobs actually running: a
// buffer can then leak for at most one sweep interval no matter how its
// job ended, including ways not invented yet.
func (l *Log) RetainOnly(keep map[string]bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for id := range l.bufs {
		if !keep[id] {
			delete(l.bufs, id)
		}
	}
}

// Len reports how many jobs currently have buffered output. Used by
// tests and useful when wondering whether Drop is actually being called.
func (l *Log) Len() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return len(l.bufs)
}
