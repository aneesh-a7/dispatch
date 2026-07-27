package livelog

import (
	"strings"
	"testing"
)

func TestReadReturnsAppendedData(t *testing.T) {
	l := New(1024)
	l.Append("j1", []byte("hello "))
	l.Append("j1", []byte("world"))

	data, from, next, truncated, ok := l.Read("j1", 0)
	if !ok {
		t.Fatal("Read ok = false, want true")
	}
	if string(data) != "hello world" {
		t.Errorf("data = %q, want %q", data, "hello world")
	}
	if from != 0 || next != 11 {
		t.Errorf("from=%d next=%d, want 0 and 11", from, next)
	}
	if truncated {
		t.Error("truncated = true on a buffer that never overflowed")
	}
}

// A follower resumes from next_offset, so reading at that offset must
// return only what arrived since, not a repeat of what it already has.
func TestReadFromOffsetReturnsOnlyNewData(t *testing.T) {
	l := New(1024)
	l.Append("j1", []byte("first"))

	_, _, next, _, _ := l.Read("j1", 0)
	l.Append("j1", []byte("second"))

	data, from, _, _, _ := l.Read("j1", next)
	if string(data) != "second" {
		t.Errorf("data = %q, want %q", data, "second")
	}
	if from != next {
		t.Errorf("from = %d, want %d", from, next)
	}
}

func TestReadWhenCaughtUpReturnsNothing(t *testing.T) {
	l := New(1024)
	l.Append("j1", []byte("abc"))

	_, _, next, _, _ := l.Read("j1", 0)
	data, _, next2, _, ok := l.Read("j1", next)
	if !ok {
		t.Fatal("Read ok = false, want true")
	}
	if len(data) != 0 {
		t.Errorf("data = %q, want empty (nothing new)", data)
	}
	if next2 != next {
		t.Errorf("next moved from %d to %d with no writes", next, next2)
	}
}

func TestReadUnknownJob(t *testing.T) {
	l := New(1024)
	if _, _, _, _, ok := l.Read("nope", 0); ok {
		t.Error("Read ok = true for a job with no buffered output")
	}
}

// Past the cap, the oldest bytes go and the newest stay: someone reading
// a long-running job's output almost always wants to know what it is
// doing now, not how it started.
func TestAppendDropsOldestPastCap(t *testing.T) {
	l := New(10)
	l.Append("j1", []byte("0123456789"))
	l.Append("j1", []byte("abcde"))

	data, from, next, _, _ := l.Read("j1", 0)
	if string(data) != "56789abcde" {
		t.Errorf("data = %q, want the last 10 bytes %q", data, "56789abcde")
	}
	if from != 5 {
		t.Errorf("from = %d, want 5 (first 5 bytes dropped)", from)
	}
	if next != 15 {
		t.Errorf("next = %d, want 15 (total bytes ever written)", next)
	}
}

// A follower that fell behind the window must be told it missed
// something, rather than silently reading across the hole.
func TestReadBehindWindowReportsTruncation(t *testing.T) {
	l := New(10)
	l.Append("j1", []byte("0123456789abcde"))

	data, from, _, truncated, ok := l.Read("j1", 0)
	if !ok {
		t.Fatal("Read ok = false, want true")
	}
	if !truncated {
		t.Error("truncated = false, want true (offset 0 was dropped)")
	}
	if from != 5 {
		t.Errorf("from = %d, want 5", from)
	}
	if string(data) != "56789abcde" {
		t.Errorf("data = %q, want what is still held", data)
	}
}

func TestDropReleasesBuffer(t *testing.T) {
	l := New(1024)
	l.Append("j1", []byte("data"))
	if l.Len() != 1 {
		t.Fatalf("Len = %d, want 1", l.Len())
	}
	l.Drop("j1")
	if l.Len() != 0 {
		t.Errorf("Len = %d after Drop, want 0", l.Len())
	}
	if _, _, _, _, ok := l.Read("j1", 0); ok {
		t.Error("Read ok = true after Drop")
	}
}

// The janitor path: whatever is not running gets freed, however it
// stopped running.
func TestRetainOnlyDropsEverythingElse(t *testing.T) {
	l := New(1024)
	for _, id := range []string{"running", "finished", "reaped"} {
		l.Append(id, []byte("x"))
	}
	l.RetainOnly(map[string]bool{"running": true})

	if l.Len() != 1 {
		t.Errorf("Len = %d, want 1", l.Len())
	}
	if _, _, _, _, ok := l.Read("running", 0); !ok {
		t.Error("running job's buffer was dropped")
	}
	for _, id := range []string{"finished", "reaped"} {
		if _, _, _, _, ok := l.Read(id, 0); ok {
			t.Errorf("%s buffer survived RetainOnly", id)
		}
	}
}

func TestRetainOnlyWithEmptySetDropsAll(t *testing.T) {
	l := New(1024)
	l.Append("j1", []byte("x"))
	l.RetainOnly(map[string]bool{})
	if l.Len() != 0 {
		t.Errorf("Len = %d, want 0", l.Len())
	}
}

func TestAppendEmptyIsNoOp(t *testing.T) {
	l := New(1024)
	l.Append("j1", nil)
	l.Append("j1", []byte{})
	if l.Len() != 0 {
		t.Errorf("Len = %d, want 0 (empty appends should not create a buffer)", l.Len())
	}
}

// Reads hand out copies; a caller keeping one must not see it change
// under them when the job keeps printing.
func TestReadReturnsIndependentCopy(t *testing.T) {
	l := New(1024)
	l.Append("j1", []byte("original"))

	data, _, _, _, _ := l.Read("j1", 0)
	data[0] = 'X'

	again, _, _, _, _ := l.Read("j1", 0)
	if string(again) != "original" {
		t.Errorf("buffer was mutated through a returned slice: %q", again)
	}
}

func TestConcurrentAppendAndRead(t *testing.T) {
	l := New(4096)
	done := make(chan struct{})

	go func() {
		defer close(done)
		for i := 0; i < 500; i++ {
			l.Append("j1", []byte(strings.Repeat("a", 10)))
		}
	}()
	for i := 0; i < 500; i++ {
		l.Read("j1", 0)
	}
	<-done

	if _, _, next, _, _ := l.Read("j1", 0); next != 5000 {
		t.Errorf("next = %d, want 5000 total bytes written", next)
	}
}
