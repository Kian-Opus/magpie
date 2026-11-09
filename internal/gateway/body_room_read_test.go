package gateway

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"testing"
	"time"
	"weak"
)

func TestDecodedSmallBodiesNeedNoTemporaryDirectory(t *testing.T) {
	path := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	setDecodedRoomTemp(t, path)
	was := bodyRoom
	b := roomOf(2 << 20)
	bodyRoom = b
	t.Cleanup(func() { bodyRoom = was })
	hold, endHold := context.WithCancel(context.Background())
	defer endHold()
	if err := b.take(hold, 2<<20); err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{300 << 10, bodyRoomFree - 1} {
		ctx, cancel := context.WithCancel(context.Background())
		plain := bytes.Repeat([]byte("x"), size)
		body, _, err := readRoomBody(ctx, bytes.NewReader(plain), int64(size), nil)
		cancel()
		if err != nil || !bytes.Equal(body, plain) {
			t.Fatalf("small body of %d bytes needed a temporary file: %v", size, err)
		}
		if held := roomHeld(b); held != 2<<20 {
			t.Fatalf("small body was charged: %d", held)
		}
	}
}

func TestDecodedBodySpoolFailures(t *testing.T) {
	plain := decodedRoomPayload()
	for _, scenario := range []string{"limit", "read error", "temporary directory"} {
		t.Run(scenario, func(t *testing.T) {
			scratch := decodedRoomTemp(t)
			was := bodyRoom
			b := roomOf(6 << 20)
			bodyRoom = b
			t.Cleanup(func() { bodyRoom = was })
			hold, endHold := context.WithCancel(context.Background())
			defer endHold()
			if err := b.take(hold, 5<<20); err != nil {
				t.Fatal(err)
			}
			packed := encodedBody(t, plain, "gzip")
			limit, want := int64(3<<20), http.StatusRequestEntityTooLarge
			switch scenario {
			case "read error":
				packed = packed[:len(packed)-8] // missing gzip trailer, after spooling starts
				limit, want = int64(len(plain)+1), http.StatusBadRequest
			case "temporary directory":
				path := filepath.Join(scratch, "file")
				if err := os.WriteFile(path, nil, 0o600); err != nil {
					t.Fatal(err)
				}
				setDecodedRoomTemp(t, path)
				want = http.StatusInternalServerError
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			r := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(packed)).WithContext(ctx)
			r.Header.Set("Content-Encoding", "gzip")
			body, status, err := readBoundedRequestBody(httptest.NewRecorder(), r, requestLimits{body: limit})
			if body != nil || err == nil || status != want {
				t.Fatalf("body=%d status=%d err=%v, want %d without a body", len(body), status, err, want)
			}
			if held := roomHeld(b); held != 5<<20 {
				t.Fatalf("failed read kept a reservation: %d", held)
			}
			files, err := os.ReadDir(scratch)
			if err != nil {
				t.Fatal(err)
			}
			for _, file := range files {
				if file.Name() != "file" {
					t.Fatalf("failed read left a temporary body: %s", file.Name())
				}
			}
		})
	}
}

// Observe only the in-memory prefix, before it is replaced by a file. A weak
// pointer lets the test check that waiting does not keep the prefix alive.
type observedRoomReader struct {
	io.Reader
	ctx     context.Context
	room    *bodyRoomT
	prefix  weak.Pointer[byte]
	arrived chan<- struct{}
	gate    <-chan struct{}
}

func (r *observedRoomReader) Read(p []byte) (int, error) {
	r.room.mu.Lock()
	inMemory := r.room.by[r.ctx.Done()] > 0
	r.room.mu.Unlock()
	if inMemory && r.arrived != nil {
		r.arrived <- struct{}{}
		r.arrived = nil
		select {
		case <-r.gate:
		case <-r.ctx.Done():
			return 0, r.ctx.Err()
		}
	}
	n, err := r.Reader.Read(p)
	if inMemory && n > 0 {
		r.prefix = weak.Make(&p[0])
	}
	return n, err
}

func TestDecodedBodyWaitingReleasesMemory(t *testing.T) {
	decodedRoomTemp(t)
	was := bodyRoom
	b := roomOf(6 << 20)
	bodyRoom = b
	t.Cleanup(func() { bodyRoom = was })
	hold, endHold := context.WithCancel(context.Background())
	t.Cleanup(endHold)
	if err := b.take(hold, 3<<20); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	plain := decodedRoomPayload()
	r := &observedRoomReader{Reader: bytes.NewReader(plain), ctx: ctx, room: b}
	exited := make(chan struct{})
	go func() {
		defer close(exited)
		_, _, err := readRoomBody(ctx, r, int64(len(plain)), nil)
		if !errors.Is(err, context.Canceled) {
			t.Errorf("canceled waiting read: %v", err)
		}
	}()
	t.Cleanup(func() { cancel(); <-exited })
	waitQueued(t, b, 1)
	runtime.GC()
	runtime.GC()
	if r.prefix.Value() != nil {
		t.Fatal("a spooled body kept its decoded prefix in memory while waiting")
	}
	runtime.KeepAlive(r)
}

func TestDecodedBodiesGrowWithoutDeadlock(t *testing.T) {
	decodedRoomTemp(t)
	was := bodyRoom
	b := roomOf(4 << 20)
	bodyRoom = b
	t.Cleanup(func() { bodyRoom = was })
	ctx, cancel := context.WithCancel(context.Background())
	var workers sync.WaitGroup
	t.Cleanup(func() { cancel(); workers.Wait() })
	plain := decodedRoomPayload()
	arrived, gate := make(chan struct{}, 3), make(chan struct{})
	type result struct {
		err  error
		same bool
	}
	got := make(chan result, 3)
	for range 3 {
		workers.Go(func() {
			ctx, cancel := context.WithCancel(ctx)
			defer cancel()
			r := &observedRoomReader{Reader: bytes.NewReader(plain), ctx: ctx, room: b, arrived: arrived, gate: gate}
			body, _, err := readRoomBody(ctx, r, int64(len(plain)), nil)
			got <- result{err, bytes.Equal(body, plain)}
		})
	}
	deadline := time.NewTimer(10 * time.Second)
	defer deadline.Stop()
	for range 3 {
		select {
		case <-arrived:
		case <-deadline.C:
			t.Fatal("requests never all held a partial reservation")
		}
	}
	close(gate)
	for range 3 {
		select {
		case result := <-got:
			if result.err != nil || !result.same {
				t.Fatalf("concurrent body changed or failed: same=%v err=%v", result.same, result.err)
			}
		case <-deadline.C:
			t.Fatal("partial reservations deadlocked while growing")
		}
	}
}
