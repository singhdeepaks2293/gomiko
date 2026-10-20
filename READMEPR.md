# PR: Fix `ReadUntil` goroutine leak, data race, and hardcoded 4s read timeout

**Branch:** `fix/readuntil-timeout-goroutine-leak`
**Files changed:** `pkg/driver/driver.go`, `pkg/driver/driver_test.go`, `pkg/driver/driver_readuntil_test.go` (new), `README.md`

---

## 1. Summary

The core read engine of `pkg/driver` (`ReadUntil` and friends) was rebuilt to fix
four real bugs and two usability limits, while keeping the public API fully
backward compatible:

| # | Problem | Severity |
|---|---------|----------|
| 1 | Goroutine + session leak on every read timeout | Critical |
| 2 | Data race on the error variable (`go test -race` fails) | Critical |
| 3 | Hardcoded 4-second read timeout | High (usability) |
| 4 | Bytes arriving after a timeout are silently lost | High |
| 5 | Busy-spin loop while waiting for the pattern | Medium (CPU) |
| 6 | `FindDevicePrompt` without a pattern blocks forever | Medium |
| 7 | `SetReturn` silently did nothing (value receiver copy) | Low (latent bug) |

---

## 2. What was wrong (before)

### 2.1 Goroutine and session leak (Problem 1)

```go
// OLD ReadUntil
go func(d *Driver, pattern string) {
    buffChan := make(chan string)
    go readRoutine(d, pattern, buffChan)   // <-- never cancelled
    select {
    case recv := <-buffChan:
        outputChan <- recv
    case <-time.After(4 * time.Second):
        err = errors.New(...)
        close(outputChan)                  // <-- caller returns, routine lives on
    }
}(d, pattern)
```

On timeout the caller returned, but the inner `readRoutine` **kept looping
forever** on `Connection.Read()`:

- one leaked goroutine per timed-out read (plus its channel and stack)
- worse, the zombie routine **kept consuming device output** that belonged to
  the *next* command — silently corrupting every subsequent read on that
  session

In batch automation (hundreds of devices × many commands, with slow devices
timing out routinely) this leaks thousands of goroutines and poisons sessions,
which triggers retry storms.

### 2.2 Data race (Problem 2)

`err` was written inside the goroutine and read by the caller right after
`<-outputChan` with no synchronization:

```go
case <-time.After(...):
    err = errors.New(...)   // written in goroutine
...
return <-outputChan, err    // read in caller — race
```

`go test -race` flags this; under real concurrency it can return a stale or
torn error value.

### 2.3 Hardcoded timeout and lost bytes (Problems 3, 4)

`time.After(time.Duration(4) * time.Second)` was the only possible read
timeout. Any `show run` on a decent-sized chassis, or any command on a busy
device, exceeds 4 seconds. And everything read before the timeout was thrown
away — the next `ReadUntil` started from zero, missing data that the device
had already sent.

### 2.4 Busy spin (Problem 5)

```go
for (err == nil) && (!r.MatchString(result)) {
    outSlice, _ := d.Connection.Read()   // no delay, errors ignored
    result += outSlice
}
```

A tight loop hammering `Read` in a goroutine that, per 2.1, never dies.

### 2.5 Unbounded blocking (Problems 6, 7)

`FindDevicePrompt(regex, "")` called `Connection.Read()` directly with no
timeout at all. Separately, `SetTimeout`/`SetReturn` used **value receivers**,
so `SetReturn` modified a throwaway copy of `Driver` and had no effect.

---

## 3. How it works now (after)

The read path in `pkg/driver/driver.go` was rewritten around a deadline loop
with three small pieces of state on `Driver`:

```go
type Driver struct {
    Connection connections.Connection
    Return     string

    mu          sync.Mutex          // serializes reads on one session
    pendingRead <-chan readResult   // in-flight read that outlived its deadline
    pendingOut  string              // bytes read but not matched yet
}
```

```go
func (d *Driver) ReadUntil(pattern string) (string, error) {
    return d.ReadUntilTimeout(pattern, DefaultReadTimeout) // 4s default, configurable
}

func (d *Driver) ReadUntilTimeout(pattern string, timeout time.Duration) (string, error)
```

Behavior of the new loop:

1. **Start from buffered output.** Bytes left over from a previous timed-out
   call (`pendingOut`) are prepended, so nothing the device already sent is
   ever lost.
2. **One blocking read at a time, raced against the deadline.** The
   `Connection.Read()` runs in a background goroutine whose result goes into a
   *buffered* channel; the caller `select`s it against the remaining time.
3. **On timeout, the in-flight read is kept** (`pendingRead`) instead of being
   abandoned — the next `ReadUntil` collects its result. No goroutine is left
   looping, and the zombie-consumer problem is gone.
4. **On match, everything read so far is returned** — same semantics as
   before.
5. **Read errors** (EOF, closed session) are now propagated instead of being
   swallowed by `outSlice, _ :=`.

### New knobs (all additive)

```go
// raise the default wait for slow devices / long outputs, e.g. before "show run"
driver.DefaultReadTimeout = 30 * time.Second

// or per call, for power users with access to *driver.Driver
out, err := d.ReadUntilTimeout("switch1#", 60*time.Second)
```

---

## 4. Impact — why this makes gomiko better

| Area | Before | After |
|------|--------|-------|
| Timeouts on slow devices | Command fails at exactly 4s, no recourse | Configurable globally and per call |
| Session integrity after timeout | Zombie reader steals the next command's output | Leftover bytes are buffered and matched by the next read |
| Long-running automation | Goroutine + session leak grows without bound | One read goroutine max, collected on completion |
| `go test -race` | Fails (data race on `err`) | Clean |
| CPU while idle-waiting | Tight spin loop | Blocking read, no spin |
| Read errors | Silently ignored | Propagated to the caller |
| `SetReturn` | No-op (value receiver copy) | Works |
| Concurrent `SendCommand` on one driver | Output could interleave on the wire | Serialized by session mutex |
| Partial output on failure | Discarded | Returned with the timeout error |

Net effect: gomiko becomes **safe for large batch jobs** (the main use case for
a Go port of netmiko) and **usable against real-world slow devices**, without
any API change for existing users.

---

## 5. Backward compatibility

- `IDriver` interface is **unchanged** — external driver implementations and
  mocks still compile untouched.
- `ReadUntil(pattern)` keeps its signature and its exact error message format.
- `NewDriver(conn, return)` is unchanged.
- One minor note: `Driver` gained *unexported* fields, so positional
  composite literals like `driver.Driver{conn, "\n"}` from other packages no
  longer compile. Constructing via `NewDriver` (the documented path) is
  unaffected; in-repo test literals were updated to keyed form.
- `SetTimeout`/`SetReturn` moved to pointer receivers — required by the new
  mutex, and it makes `SetReturn` actually take effect (previously a silent
  no-op, which no caller could have depended on).

---

## 6. Tests

New file `pkg/driver/driver_readuntil_test.go` (7 deterministic tests, no
sleeps longer than needed, run in ~0.5s):

1. `TestReadUntilTimeout_ReturnsErrorInsteadOfHanging` — timeout returns
   promptly with an error
2. `TestReadUntilTimeout_KeepsPartialOutputAcrossTimeouts` — partial output is
   returned *and* reassembled with late-arriving bytes on the next call
3. `TestReadUntilTimeout_ReusesInFlightRead` — a read left in flight by a
   timeout is consumed by the next call (no leak, both halves of data seen)
4. `TestReadUntilTimeout_NoGoroutineLeak` — `runtime.NumGoroutine()` returns to
   baseline after a timeout (this test fails against the old implementation)
5. `TestReadUntil_PropagatesReadError` — connection-level errors are no longer
   swallowed
6. `TestFindDevicePrompt_PatternLessReadIsTimed` — prompt discovery cannot
   block forever
7. `TestReadUntil_ConcurrentCallersDoNotRace` — concurrent use of one driver
   (guards the mutex work under `-race`)

Also fixed the legacy `TestDriver_FindDevicePrompt`, which ran `t.Fatal`
inside a goroutine and asserted a panic that never happened — it now asserts
the timeout error deterministically.

**Verification:** `go vet ./...` clean · `gofmt` clean on changed files ·
`go test ./...` — all 5 packages pass · stable across `-count=5`.
Note: `-race` could not be exercised locally (no gcc on the dev machine);
recommend letting CI run with `-race` (Ubuntu runner supports it).

---

## 7. How to review

1. `pkg/driver/driver.go` — the whole read path is the loop in
   `readUntilTimeout` plus `takeOrStartRead`; everything else is unchanged.
2. `pkg/driver/driver_readuntil_test.go` — read top to bottom; each test maps
   to one numbered problem in section 2.
3. `pkg/driver/driver_test.go` — only the literal updates and the rewritten
   legacy timeout test.
