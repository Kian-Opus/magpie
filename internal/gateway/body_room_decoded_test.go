package gateway

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"math/rand/v2"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yetone/magpie/internal/access"
	"github.com/yetone/magpie/internal/budget"
	"github.com/yetone/magpie/internal/provider"
)

func TestBodyRoomGrowthDoesNotExceedRoom(t *testing.T) {
	b := roomOf(4 << 20)
	first, endFirst := context.WithCancel(context.Background())
	second, endSecond := context.WithCancel(context.Background())
	defer endFirst()
	defer endSecond()
	if err := b.take(first, 1<<20); err != nil {
		t.Fatal(err)
	}
	if err := b.take(second, 2<<20); err != nil {
		t.Fatal(err)
	}
	if err := b.take(first, 3<<20); err == nil || roomHeld(b) != 3<<20 {
		t.Fatalf("growing a reservation bypassed the room: err=%v held=%d", err, roomHeld(b))
	}
}

func TestBodyRoomValueContextsShareReservation(t *testing.T) {
	b := roomOf(2 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	was := bodyRoomWait
	bodyRoomWait = 5 * time.Millisecond
	t.Cleanup(func() { bodyRoomWait = was })
	if err := b.take(ctx, 2<<20); err != nil {
		t.Fatal(err)
	}
	type callerKey struct{}
	withCaller := context.WithValue(ctx, callerKey{}, "same request")
	if err := b.take(withCaller, 2<<20); err != nil || roomHeld(b) != 2<<20 {
		t.Fatalf("one request was charged twice after a guard added context: %v, held=%d", err, roomHeld(b))
	}
}

// A realistic JSON body with an incompressible base64 input followed by text:
// its wire length is above the exemption, but smaller than its decoded size.
func decodedRoomPayload() []byte {
	noise := make([]byte, 3<<19)
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range noise {
		noise[i] = byte(rng.Uint64())
	}
	return []byte(`{"model":"fake/m1","input":"` + base64.StdEncoding.EncodeToString(noise) + strings.Repeat("x", 2<<20) + `"}`)
}

func decodedRoomTemp(t *testing.T) string {
	t.Helper()
	dir := t.TempDir()
	setDecodedRoomTemp(t, dir)
	return dir
}

func setDecodedRoomTemp(t *testing.T, dir string) {
	t.Helper()
	for _, name := range []string{"TMPDIR", "TMP", "TEMP"} {
		t.Setenv(name, dir)
	}
}

type decodedRoomDeadlineRecorder struct {
	*httptest.ResponseRecorder
	deadline atomic.Int64
}

func (w *decodedRoomDeadlineRecorder) SetReadDeadline(at time.Time) error {
	var n int64
	if !at.IsZero() {
		n = at.UnixNano()
	}
	w.deadline.Store(n)
	return nil
}

type decodedRoomRead struct {
	body   []byte
	ok     bool
	status int
	text   string
}

func TestDecodedBodyWaitsWithoutRetainingItsBody(t *testing.T) {
	plain := decodedRoomPayload()
	for _, encoding := range []string{"gzip", "zstd"} {
		for _, canceled := range []bool{false, true} {
			name := encoding
			if canceled {
				name += "/canceled"
			}
			t.Run(name, func(t *testing.T) {
				scratch := decodedRoomTemp(t)
				was := bodyRoom
				b := roomOf(6 << 20)
				bodyRoom = b
				t.Cleanup(func() { bodyRoom = was })
				hold, endHold := context.WithCancel(context.Background())
				t.Cleanup(endHold)
				if err := b.take(hold, 3<<20); err != nil {
					t.Fatal(err)
				}
				packed := encodedBody(t, plain, encoding)
				if len(packed) < bodyRoomFree || len(packed) >= 3<<20 {
					t.Fatalf("fixture wire size %d does not fit beside the held body", len(packed))
				}
				ctx, cancel := context.WithCancel(context.Background())
				r := httptest.NewRequest(http.MethodPost, "/v1/responses", bytes.NewReader(packed)).WithContext(ctx)
				r.Header.Set("Content-Encoding", encoding)
				got := make(chan decodedRoomRead, 1)
				exited := make(chan struct{})
				w := &decodedRoomDeadlineRecorder{ResponseRecorder: httptest.NewRecorder()}
				go func() {
					defer close(exited)
					body, ok := (&Server{}).requestBody(w, r, provider.Responses)
					got <- decodedRoomRead{body, ok, w.Code, w.Body.String()}
				}()
				t.Cleanup(func() { cancel(); <-exited })
				deadline := time.NewTimer(10 * time.Second)
				defer deadline.Stop()
				tick := time.NewTicker(time.Millisecond)
				defer tick.Stop()
				for {
					b.mu.Lock()
					queued := len(b.queue)
					b.mu.Unlock()
					if queued != 0 {
						break
					}
					select {
					case result := <-got:
						t.Fatalf("decoded body bypassed the room: ok=%v status=%d held=%d", result.ok, result.status, roomHeld(b))
					case <-deadline.C:
						t.Fatal("decoded body never reached room admission")
					case <-tick.C:
					}
				}
				if w.deadline.Load() != 0 {
					t.Fatal("upload deadline is still active while the complete body waits for room")
				}
				if held := roomHeld(b); held != 3<<20 {
					t.Fatalf("a waiting decoded body still holds %d bytes of room", held-(3<<20))
				}
				files, err := os.ReadDir(scratch)
				if err != nil || len(files) != 1 {
					t.Fatalf("the waiting body was not spooled: files=%d err=%v", len(files), err)
				}
				if canceled {
					cancel()
				} else {
					endHold()
				}
				result := <-got
				if canceled {
					if result.ok || result.body != nil || result.status != http.StatusServiceUnavailable || !strings.Contains(result.text, context.Canceled.Error()) {
						t.Fatalf("canceled admission: ok=%v status=%d %s", result.ok, result.status, result.text)
					}
				} else if !result.ok || !bytes.Equal(result.body, plain) {
					t.Fatalf("admitted body changed: ok=%v status=%d bytes=%d", result.ok, result.status, len(result.body))
				}
				if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
					t.Fatalf("temporary body was not removed: files=%d err=%v", len(files), err)
				}
			})
		}
	}
}

// A spent limit on another provider makes withSpent add caller values after
// the key guard decodes the body. The serving route must share that room.
func TestDecodedBodyKeyGuardSharesRoom(t *testing.T) {
	fresh(t)
	budget.Forget()
	pinLimitClock(t)
	scratch := decodedRoomTemp(t)
	plan := &fake{t: t, ctype: "application/json", reply: usedReply("from-plan")}
	spare := &fake{t: t, ctype: "application/json", reply: usedReply("from-spare")}
	twoProviders(t, plan, spare)
	keys, secrets := newCaller(t, "Capped")
	setLimit(t, keys[0].ID, &access.Limit{Period: "day", Tokens: 350, Models: []string{"spare/*"}})
	h := New().Handler()
	if w := keyCall(h, secrets[0], "/v1/chat/completions", `{"model":"spare/m2","messages":[{"role":"user","content":"hi"}]}`); w.Code != http.StatusOK {
		t.Fatal(w.Code, w.Body.String())
	}
	plain := []byte(`{"model":"plan/m1","messages":[{"role":"user","content":"` + strings.Repeat("x", 2<<20) + `"}]}`)
	was, wasWait := bodyRoom, bodyRoomWait
	bodyRoom, bodyRoomWait = roomOf(int64(len(plain))), 50*time.Millisecond
	t.Cleanup(func() { bodyRoom, bodyRoomWait = was, wasWait })
	for _, encoding := range []string{"gzip", "zstd"} {
		ctx, cancel := context.WithCancel(context.Background())
		r := httptest.NewRequest(http.MethodPost, "/v1/chat/completions", bytes.NewReader(encodedBody(t, plain, encoding))).WithContext(ctx)
		r.Header.Set("Authorization", "Bearer "+secrets[0])
		r.Header.Set("Content-Encoding", encoding)
		w := httptest.NewRecorder()
		h.ServeHTTP(w, r)
		cancel()
		if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "from-plan") {
			t.Fatalf("%s limited key: %d %s", encoding, w.Code, w.Body.String())
		}
	}
	if plan.calls != 2 || spare.calls != 1 {
		t.Fatalf("wrong provider calls: plan=%d spare=%d", plan.calls, spare.calls)
	}
	if files, err := os.ReadDir(scratch); err != nil || len(files) != 0 {
		t.Fatalf("a fitting body touched the spool: files=%d err=%v", len(files), err)
	}
}

func TestBodyRoomCanceledContextTakesNothing(t *testing.T) {
	b := roomOf(2 << 20)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := b.take(ctx, 1<<20); !errors.Is(err, context.Canceled) || roomHeld(b) != 0 {
		t.Fatalf("canceled request was admitted: err=%v held=%d", err, roomHeld(b))
	}
}
