package driver

import (
	"io"
	"runtime"
	"sync"
	"testing"
	"time"
)

// scriptChunk is one scripted outcome of a Read call.
type scriptChunk struct {
	data string
	err  error
}

// scriptedConn lets a test decide exactly what each Read call returns and
// when. A receive on an empty channel models a device that has not sent
// anything yet (Read blocks).
type scriptedConn struct {
	ch chan scriptChunk
}

func (c *scriptedConn) Connect() error           { return nil }
func (c *scriptedConn) Disconnect()              {}
func (c *scriptedConn) SetTimeout(timeout uint8) {}
func (c *scriptedConn) Write(cmd string) int     { return len(cmd) }

func (c *scriptedConn) Read() (string, error) {
	chunk, ok := <-c.ch
	if !ok {
		return "", io.EOF
	}
	return chunk.data, chunk.err
}

// blockingConn models a completely idle device: Read blocks until release
// is closed, then finally responds.
type blockingConn struct {
	release chan struct{}
}

func (c *blockingConn) Connect() error           { return nil }
func (c *blockingConn) Disconnect()              {}
func (c *blockingConn) SetTimeout(timeout uint8) {}
func (c *blockingConn) Write(cmd string) int     { return len(cmd) }

func (c *blockingConn) Read() (string, error) {
	<-c.release
	return "switch1#", nil
}

func TestReadUntilTimeout_ReturnsErrorInsteadOfHanging(t *testing.T) {
	conn := &scriptedConn{ch: make(chan scriptChunk, 1)}
	d := &Driver{Connection: conn, Return: "\n"}

	start := time.Now()
	_, err := d.ReadUntilTimeout("switch1#", 100*time.Millisecond)
	elapsed := time.Since(start)

	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}
	if elapsed > time.Second {
		t.Errorf("ReadUntilTimeout should return shortly after the timeout, took %v", elapsed)
	}

	// release the background read so it can finish
	conn.ch <- scriptChunk{data: "late data"}
}

func TestReadUntilTimeout_KeepsPartialOutputAcrossTimeouts(t *testing.T) {
	conn := &scriptedConn{ch: make(chan scriptChunk, 1)}
	d := &Driver{Connection: conn, Return: "\n"}

	// first read returns only part of the expected output, then times out
	conn.ch <- scriptChunk{data: "swi"}
	out, err := d.ReadUntilTimeout("switch1#", 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected a timeout error on the first read")
	}
	if out != "swi" {
		t.Errorf("expected partial output %q, got %q", "swi", out)
	}

	// the remainder arrives after the deadline; a new read must still
	// reassemble the full output instead of losing the partial bytes
	go func() {
		time.Sleep(100 * time.Millisecond)
		conn.ch <- scriptChunk{data: "tch1#"}
	}()

	out, err = d.ReadUntilTimeout("switch1#", 2*time.Second)
	if err != nil {
		t.Fatalf("expected pattern to match after late data, got error: %v", err)
	}
	if out != "switch1#" {
		t.Errorf("expected reassembled output %q, got %q", "switch1#", out)
	}
}

func TestReadUntilTimeout_ReusesInFlightRead(t *testing.T) {
	// unbuffered channel: Read blocks until the test feeds data, which
	// leaves a read in flight when the first wait times out
	conn := &scriptedConn{ch: make(chan scriptChunk)}
	d := &Driver{Connection: conn, Return: "\n"}

	_, err := d.ReadUntilTimeout("switch1#", 50*time.Millisecond)
	if err == nil {
		t.Fatal("expected a timeout error, got nil")
	}

	// feed the prompt in two halves; the in-flight read from the timed-out
	// wait must be reused, not leaked, and both halves must be seen
	fed := make(chan struct{})
	go func() {
		defer close(fed)
		conn.ch <- scriptChunk{data: "swi"}
		conn.ch <- scriptChunk{data: "tch1#"}
	}()

	out, err := d.ReadUntilTimeout("switch1#", 2*time.Second)
	if err != nil {
		t.Fatalf("expected pattern to match, got error: %v", err)
	}
	if out != "switch1#" {
		t.Errorf("expected output %q from reused read, got %q", "switch1#", out)
	}
	<-fed
}

func TestReadUntilTimeout_NoGoroutineLeak(t *testing.T) {
	before := runtime.NumGoroutine()

	conn := &blockingConn{release: make(chan struct{})}
	d := &Driver{Connection: conn, Return: "\n"}

	if _, err := d.ReadUntilTimeout("switch1#", 50*time.Millisecond); err == nil {
		t.Fatal("expected a timeout error, got nil")
	}

	// the device finally responds; the read that was in flight at timeout
	// must complete and exit instead of looping on the connection forever
	close(conn.release)

	deadline := time.Now().Add(2 * time.Second)
	for runtime.NumGoroutine() > before && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if after := runtime.NumGoroutine(); after > before {
		t.Errorf("goroutines leaked: %d before timeout, %d after", before, after)
	}
}

func TestReadUntil_PropagatesReadError(t *testing.T) {
	conn := &scriptedConn{ch: make(chan scriptChunk, 1)}
	d := &Driver{Connection: conn, Return: "\n"}

	conn.ch <- scriptChunk{err: io.EOF}
	_, err := d.ReadUntil("switch1#")
	if err != io.EOF {
		t.Errorf("expected connection read error to be propagated, got %v", err)
	}
}

func TestFindDevicePrompt_PatternLessReadIsTimed(t *testing.T) {
	// a device that never responds must not block FindDevicePrompt forever
	oldTimeout := DefaultReadTimeout
	DefaultReadTimeout = 100 * time.Millisecond
	defer func() { DefaultReadTimeout = oldTimeout }()

	conn := &scriptedConn{ch: make(chan scriptChunk)}
	d := &Driver{Connection: conn, Return: "\n"}

	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		_, err = d.FindDevicePrompt("\\r?(.*)[#>]", "")
	}()

	select {
	case <-done:
		if err == nil {
			t.Error("expected an error for a prompt read that timed out")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("FindDevicePrompt blocked past its timeout")
	}

	// unblock the parked read so its goroutine can finish
	conn.ch <- scriptChunk{data: "switch1#"}
}

func TestReadUntil_ConcurrentCallersDoNotRace(t *testing.T) {
	// one Driver per goroutine is the supported model; this guards the
	// internal pending-read bookkeeping when combined with go test -race
	conn := &scriptedConn{ch: make(chan scriptChunk, 64)}
	d := &Driver{Connection: conn, Return: "\n"}

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			conn.ch <- scriptChunk{data: "switch1#"}
			_, _ = d.ReadUntil("switch1#")
		}()
	}
	wg.Wait()
}
