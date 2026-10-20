package driver

import (
	"errors"
	"regexp"
	"sync"
	"time"

	"github.com/Ali-aqrabawi/gomiko/pkg/connections"
)

// DefaultReadTimeout is the maximum time ReadUntil waits for the expect
// pattern to appear before it gives up. Increase it globally when working
// with slow devices or long-running commands (for example "show run"):
//
//	driver.DefaultReadTimeout = 30 * time.Second
var DefaultReadTimeout = 4 * time.Second

// readResult carries the outcome of one blocking Connection.Read call back
// to whichever read wait is currently listening for it.
type readResult struct {
	data string
	err  error
}

type Driver struct {
	Connection connections.Connection
	Return     string `defaults:"\n"`

	// mu serializes reads on the session so that concurrent SendCommand
	// calls on one driver cannot interleave on the wire.
	mu sync.Mutex

	// pendingRead keeps a Connection.Read call that is still in flight after
	// its wait timed out, so its result is collected by the next read instead
	// of being lost.
	pendingRead <-chan readResult

	// pendingOut keeps bytes that were already read but did not match the
	// requested pattern, so a later read can still see them.
	pendingOut string
}

func (d *Driver) Connect() error {
	err := d.Connection.Connect()
	return err

}

func (d *Driver) Disconnect() {
	d.Connection.Disconnect()

}

func (d *Driver) SendCommand(cmd string, expectPattern string) (string, error) {
	if d.Connection == nil {
		return "", errors.New("not connected to device, make sure to call .Connect() first")
	}

	cmd += d.Return

	d.Connection.Write(cmd)

	result, err := d.ReadUntil(expectPattern)

	return result, err

}

func (d *Driver) SendCommandsSet(cmds []string, expectPattern string) (string, error) {
	if d.Connection == nil {
		return "", errors.New("not connected to device, make sure to call .Connect() first")
	}
	var results string

	for _, cmd := range cmds {
		out, _ := d.SendCommand(cmd, expectPattern)
		results += out
	}
	return results, nil

}

func (d *Driver) FindDevicePrompt(regex string, pattern string) (string, error) {
	var out string
	var err error

	r, err := regexp.Compile(regex)
	if err != nil {
		return "", err
	}

	d.mu.Lock()
	defer d.mu.Unlock()

	if pattern != "" {
		out, err = d.readUntilTimeout(pattern, DefaultReadTimeout)
		if err != nil {
			return "", err
		}
	} else {
		out, err = d.readOnce(DefaultReadTimeout)
		if err != nil {
			return "", err
		}
	}

	if !r.MatchString(out) {
		return "", errors.New("failed to find prompt, pattern: " + pattern + " , output: " + out)
	}
	return r.FindStringSubmatch(out)[1], nil

}

// ReadUntil reads from the device until the expect pattern appears or
// DefaultReadTimeout elapses.
func (d *Driver) ReadUntil(pattern string) (string, error) {
	return d.ReadUntilTimeout(pattern, DefaultReadTimeout)
}

// ReadUntilTimeout is ReadUntil with a caller-supplied timeout, which makes
// it suitable for long-running commands on slow devices.
func (d *Driver) ReadUntilTimeout(pattern string, timeout time.Duration) (string, error) {
	d.mu.Lock()
	defer d.mu.Unlock()

	return d.readUntilTimeout(pattern, timeout)
}

func (d *Driver) readUntilTimeout(pattern string, timeout time.Duration) (string, error) {
	r, err := regexp.Compile(pattern)
	if err != nil {
		return "", err
	}

	deadline := time.Now().Add(timeout)

	// Start from output that was read but not matched by a previous call,
	// so bytes received after that call's deadline are not lost.
	output := d.pendingOut
	d.pendingOut = ""

	for {
		if r.MatchString(output) {
			return output, nil
		}

		remaining := time.Until(deadline)
		if remaining <= 0 {
			d.pendingOut = output
			return output, readTimeoutError(pattern)
		}

		// Reuse the in-flight read if a previous wait timed out while a read
		// was still pending, otherwise start a new one in the background and
		// race it against the remaining time.
		read := d.takeOrStartRead()
		select {
		case res := <-read:
			output += res.data
			if res.err != nil {
				// Connection-level failure (EOF, closed session, ...). Keep
				// whatever was read so far, it may already match.
				if r.MatchString(output) {
					return output, nil
				}
				d.pendingOut = output
				return output, res.err
			}
		case <-time.After(remaining):
			// The background read is still in flight; remember it so its
			// result is collected by the next read instead of leaking.
			d.pendingRead = read
			d.pendingOut = output
			return output, readTimeoutError(pattern)
		}
	}
}

// readOnce performs a single timed read instead of blocking forever.
func (d *Driver) readOnce(timeout time.Duration) (string, error) {
	if d.pendingOut != "" {
		out := d.pendingOut
		d.pendingOut = ""
		return out, nil
	}

	read := d.takeOrStartRead()
	select {
	case res := <-read:
		return res.data, res.err
	case <-time.After(timeout):
		d.pendingRead = read
		return "", errors.New("timeout while waiting for device output")
	}
}

// takeOrStartRead returns the in-flight read if there is one, otherwise it
// starts a new background read and returns its channel. The channel is
// buffered so the background read never blocks on delivery.
func (d *Driver) takeOrStartRead() <-chan readResult {
	if d.pendingRead != nil {
		read := d.pendingRead
		d.pendingRead = nil
		return read
	}

	read := make(chan readResult, 1)
	go func() {
		data, err := d.Connection.Read()
		read <- readResult{data: data, err: err}
	}()
	return read
}

func readTimeoutError(pattern string) error {
	return errors.New("timeout while reading, read pattern not found pattern: " + pattern)
}

func (d *Driver) SetTimeout(timeout uint8) {
	d.Connection.SetTimeout(timeout)
}

func (d *Driver) SetReturn(Return string) {
	d.Return = Return
}
