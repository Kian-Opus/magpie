package gateway

import (
	"context"
	"math/rand/v2"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"unsafe"
	"weak"
)

// Large diagnostic bodies remain readable while the ring lets their plain
// allocations go. The request being served still owns its original copy.
func TestRecentBodiesReleasePlainCopies(t *testing.T) {
	s := &Server{}
	var refs []weak.Pointer[byte]
	for i := range recentMost {
		c := recentBodyCall(i)
		refs = append(refs, weak.Make(unsafe.StringData(c.RequestBody)), weak.Make(unsafe.StringData(c.ResponseBody)))
		s.record(c)
	}
	runtime.GC()
	runtime.GC()
	alive := 0
	for _, ref := range refs {
		if ref.Value() != nil {
			alive++
		}
	}
	if alive != 0 {
		t.Errorf("recent calls still retain %d large plain body allocations", alive)
	}
	for i := range recentMost {
		want := recentBodyCall(i)
		byID, ok := s.RecentCall(uint64(i + 1))
		if !ok || byID.RequestBody != want.RequestBody || byID.ResponseBody != want.ResponseBody || !byID.RequestTruncated || !byID.ResponseTruncated {
			t.Fatalf("call %d did not preserve its bodies and truncation flags", i)
		}
		byRoute, ok := s.RouteCall(want.Route)
		if !ok || byRoute.RequestBody != want.RequestBody || byRoute.ResponseBody != want.ResponseBody {
			t.Fatalf("route %d did not preserve its bodies", want.Route)
		}
	}
	for i, c := range s.Recent() {
		want := recentBodyCall(recentMost - i - 1)
		if c.Model != want.Model || c.RequestBody != want.RequestBody || c.ResponseBody != want.ResponseBody {
			t.Fatalf("recent call %d did not preserve its order or bodies", i)
		}
	}
	runtime.KeepAlive(s)
}

func TestRecentBodiesLeaveExportStateWithServingCall(t *testing.T) {
	in, out := []byte("whole request"), []byte("whole response")
	masked := &laterMasked{ctx: context.Background(), plain: []byte("request to mask later")}
	c := Call{RequestBody: "request", ResponseBody: "response", wholeIn: in, wholeOut: out, maskIn: masked}
	s := &Server{}
	s.record(c)
	got, ok := s.RecentCall(1)
	if !ok {
		t.Fatal("recorded call missing")
	}
	if got.wholeIn != nil || got.wholeOut != nil || got.maskIn != nil {
		t.Fatal("recent call retained export-only bodies or their request context")
	}
	if string(c.wholeIn) != "whole request" || string(c.wholeOut) != "whole response" || c.maskIn != masked || string(masked.plain) != "request to mask later" {
		t.Fatal("recording changed the serving call's export state")
	}
}

func TestRecentBodiesRoundTrip(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	binary := make([]byte, callBodyLimit)
	for i := range binary {
		binary[i] = byte(rng.Uint64())
	}
	for _, body := range []string{"", "short", strings.Repeat("x", 4<<10), strings.Repeat("y", (4<<10)+1), string(binary), strings.Repeat("中文🙂", callBodyLimit/10)} {
		s := &Server{}
		c := Call{Route: 99, RequestBody: body, ResponseBody: body}
		s.record(c)
		got, ok := s.RecentCall(1)
		if !ok || got.RequestBody != body || got.ResponseBody != body || got.RequestTruncated || got.ResponseTruncated {
			t.Fatalf("%d-byte body changed", len(body))
		}
		if c.RequestBody != body || c.ResponseBody != body {
			t.Fatal("recording changed the serving call's bodies")
		}
		if _, ok := s.RouteCall(0); ok {
			t.Fatal("route zero matched a call")
		}
		if _, ok := s.RecentCall(2); ok {
			t.Fatal("unknown call matched")
		}
	}
}

func TestRecentBodiesSummariesDoNotDecode(t *testing.T) {
	s := &Server{}
	for i := range recentMost {
		s.record(recentBodyCall(i))
	}
	var calls []Call
	if n := allocated(func() { calls = s.RecentSummaries() }); n > 128<<10 {
		t.Fatalf("a summary allocated %d bytes for 40 calls; their bodies should stay unread", n)
	}
	if len(calls) != recentMost {
		t.Fatalf("got %d summaries, want %d", len(calls), recentMost)
	}
	for i, c := range calls {
		if c.ID != uint64(recentMost-i) || c.RequestBody != "" || c.ResponseBody != "" || !c.RequestTruncated || !c.ResponseTruncated {
			t.Fatalf("summary %d lost its metadata or included a body", i)
		}
	}
	// A summary kept by the page must not keep an evicted body's storage alive.
	var refs []weak.Pointer[byte]
	for _, c := range s.recent {
		refs = append(refs, weak.Make(unsafe.StringData(c.packedRequest)), weak.Make(unsafe.StringData(c.packedResponse)))
	}
	for range recentMost {
		s.record(Call{Status: 200})
	}
	runtime.GC()
	runtime.GC()
	for _, ref := range refs {
		if ref.Value() != nil {
			t.Fatal("a summary retained an evicted body's compressed storage")
		}
	}
	runtime.KeepAlive(calls)
	runtime.KeepAlive(s)
}

func TestRecentBodiesConcurrentReaders(t *testing.T) {
	s := &Server{}
	base := recentBodyCall(0)
	base.RequestBody = strings.Clone(base.RequestBody[:16<<10])
	base.ResponseBody = strings.Clone(base.ResponseBody[:16<<10])
	s.record(base)
	var readers sync.WaitGroup
	for reader := range 3 {
		readers.Go(func() {
			for i := range 40 {
				if reader == 0 {
					c := base
					c.Route = int64(i + 1)
					s.record(c)
					continue
				}
				for _, summary := range s.RecentSummaries() {
					c, ok := s.RecentCall(summary.ID)
					if reader == 2 {
						c, ok = s.RouteCall(summary.Route)
					}
					if ok && (c.RequestBody != base.RequestBody || c.ResponseBody != base.ResponseBody) {
						t.Error("concurrent recording changed a call's body")
						return
					}
				}
			}
		})
	}
	readers.Wait()
}

func recentBodyCall(i int) Call {
	label := strconv.Itoa(i)
	request := `{"model":"test/model","messages":[{"role":"user","content":"` + strings.Repeat("Inspect the request history and preserve the diagnostic details. "+label+"\\n", 4000) + `"}]}`
	response := strings.Repeat("data: {\"choices\":[{\"delta\":{\"content\":\"检查内存，保留完整诊断信息。"+label+"\"}}]}\n\n", 4000)
	return Call{Route: int64(i + 100), Model: "model-" + label, Status: 200,
		RequestBody: strings.Clone(request[:callBodyLimit]), ResponseBody: strings.Clone(response[:callBodyLimit]),
		RequestTruncated: true, ResponseTruncated: true}
}

// Run with -benchtime=1x: each iteration measures a full ring after collecting
// its temporary work, separate from allocations made while recording calls.
func BenchmarkRecentBodiesRetained(b *testing.B) {
	for range b.N {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		s := &Server{}
		for i := range recentMost {
			s.record(recentBodyCall(i))
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "retained-B")
		runtime.KeepAlive(s)
	}
}
