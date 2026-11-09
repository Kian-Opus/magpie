package gateway

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"runtime"
	"sync"
	"testing"
	"testing/synctest"
	"time"
	"weak"
)

// The same image is both a tool result and a downloadable file. Keeping the
// result must not also keep a base64 copy of each file for the whole hour.
func TestImageMCPJobsKeepOneImageCopy(t *testing.T) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	s := &Server{}
	for range 8 {
		imageCacheJob(t, s, 1<<20)
	}
	runtime.GC()
	runtime.GC()
	runtime.ReadMemStats(&after)
	if retained := int64(after.HeapAlloc) - int64(before.HeapAlloc); retained > 12<<20 {
		t.Errorf("8 MiB of images retained %d bytes; completed jobs must share the files' image storage", retained)
	}
	runtime.KeepAlive(s)
}

func imageCacheJob(t testing.TB, s *Server, size int) *imageJob {
	t.Helper()
	j := s.imageMCPStart("", nil, func() map[string]any { return imageCacheDraw(s, size) })
	<-j.done
	s.imageStore.running.Wait()
	return j
}

func imageCacheDraw(s *Server, size int) map[string]any {
	img := make([]byte, size)
	copy(img, pngBytes)
	body, err := json.Marshal(map[string]any{"model": "art/test", "data": []any{map[string]any{"b64_json": base64.StdEncoding.EncodeToString(img)}}})
	if err != nil {
		panic(err)
	}
	rec := &replyRecorder{header: http.Header{}}
	rec.WriteHeader(http.StatusOK)
	rec.Write(body)
	return s.imageMCPDrawn(rec, "", "http://127.0.0.1:3425")
}

func TestImageMCPJobDoesNotRestoreEvictedFiles(t *testing.T) {
	s := &Server{}
	ready, finish := make(chan struct{}), make(chan struct{})
	j := s.imageMCPStart("", nil, func() map[string]any {
		result := imageCacheDraw(s, 4<<10)
		close(ready)
		<-finish
		return result
	})
	<-ready
	// Another drawing fills the cache before this result is published.
	other := imageCacheJobWithoutWaiting(t, s, 4<<10)
	s.imageStore.mu.Lock()
	s.imageStore.prune(time.Now(), 4<<10)
	s.imageStore.mu.Unlock()
	close(finish)
	<-j.done
	s.imageStore.running.Wait()
	r := httptest.NewRequest(http.MethodPost, ImageMCPPath, nil)
	if s.imageMCPResult(r, j.id)["isError"] != true {
		t.Fatal("a late result restored image bytes outside the file budget")
	}
	assertImageCacheReply(t, s, s.imageMCPAwait(r, j), 4<<10, false)
	assertImageCacheReply(t, s, s.imageMCPResult(r, other.id), 4<<10, true)
}

func imageCacheJobWithoutWaiting(t testing.TB, s *Server, size int) *imageJob {
	t.Helper()
	j := s.imageMCPStart("", nil, func() map[string]any { return imageCacheDraw(s, size) })
	<-j.done
	return j
}

func TestImageMCPCleanupDoesNotRetainUnusedServer(t *testing.T) {
	ref := func() weak.Pointer[Server] {
		s := &Server{}
		imageCacheJob(t, s, 4<<10)
		return weak.Make(s)
	}()
	runtime.GC()
	runtime.GC()
	if ref.Value() != nil {
		t.Fatal("the expiry timer kept an unused server and its image cache alive")
	}
}

func TestImageMCPJobEvictionReleasesResult(t *testing.T) {
	s := &Server{}
	first := imageCacheJob(t, s, 4<<10).id
	second := imageCacheJob(t, s, 4<<10).id
	third := imageCacheJob(t, s, 4<<10).id
	// A response already being delivered survives cache eviction unchanged.
	r := httptest.NewRequest(http.MethodPost, ImageMCPPath, nil)
	held := s.imageMCPResult(r, first)
	ref := func() weak.Pointer[byte] {
		s.imageStore.mu.Lock()
		defer s.imageStore.mu.Unlock()
		for _, f := range s.imageStore.files {
			if f.job == first {
				return weak.Make(&f.data[0])
			}
		}
		t.Fatal("the first job has no file")
		return weak.Pointer[byte]{}
	}()
	s.imageStore.mu.Lock()
	s.imageStore.prune(time.Now(), 8<<10)
	s.imageStore.armCleanup()
	kept := s.imageStore.bytes
	s.imageStore.mu.Unlock()
	if kept != 8<<10 || s.imageMCPResult(r, first)["isError"] != true {
		t.Fatalf("eviction kept %d bytes or left the evicted job retrievable", kept)
	}
	assertImageCacheReply(t, s, held, 4<<10, false)
	held = nil
	for _, id := range []string{second, third} {
		assertImageCacheReply(t, s, s.imageMCPResult(r, id), 4<<10, true)
	}
	runtime.GC()
	runtime.GC()
	if ref.Value() != nil {
		t.Error("an evicted image is still retained by its completed job")
	}
	runtime.KeepAlive(s)
}

func TestImageMCPCacheExpiresWithoutAnotherRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		s := &Server{}
		first := imageCacheJob(t, s, 4<<10).id
		// A running job must not be dropped by completed-job expiry.
		gate := make(chan struct{})
		pending := s.imageMCPStart("", nil, func() map[string]any {
			<-gate
			return toolResult("done", false)
		})
		defer func() { close(gate); s.imageStore.running.Wait() }()
		time.Sleep(imageMCPKeep / 2)
		// This job does not wait on running: the deliberately held job above
		// stays in flight while time advances in this test.
		second := s.imageMCPStart("", nil, func() map[string]any { return toolResult("second", false) })
		<-second.done
		time.Sleep(imageMCPKeep/2 + time.Second)
		synctest.Wait()
		s.imageStore.mu.Lock()
		_, firstKept := s.imageStore.jobs[first]
		_, secondKept := s.imageStore.jobs[second.id]
		_, pendingKept := s.imageStore.jobs[pending.id]
		files, size := len(s.imageStore.files), s.imageStore.bytes
		s.imageStore.mu.Unlock()
		if firstKept || !secondKept || !pendingKept || files != 0 || size != 0 {
			t.Fatalf("idle expiry: first=%v second=%v pending=%v files=%d bytes=%d", firstKept, secondKept, pendingKept, files, size)
		}
		time.Sleep(imageMCPKeep / 2)
		synctest.Wait()
		s.imageStore.mu.Lock()
		_, secondKept = s.imageStore.jobs[second.id]
		_, pendingKept = s.imageStore.jobs[pending.id]
		s.imageStore.mu.Unlock()
		if secondKept || !pendingKept {
			t.Fatalf("next expiry: second=%v pending=%v", secondKept, pendingKept)
		}
		runtime.KeepAlive(s)
	})
}

func TestImageMCPJobConcurrentDeliveryAndEviction(t *testing.T) {
	s := &Server{}
	imageCacheJob(t, s, 4<<10)
	var readers sync.WaitGroup
	for range 3 {
		readers.Go(func() {
			for range 30 {
				s.imageStore.mu.Lock()
				var ids []string
				for id := range s.imageStore.jobs {
					ids = append(ids, id)
				}
				s.imageStore.mu.Unlock()
				for _, id := range ids {
					r := httptest.NewRequest(http.MethodPost, ImageMCPPath, nil)
					if _, err := json.Marshal(s.imageMCPResult(r, id)); err != nil {
						t.Error(err)
					}
				}
			}
		})
	}
	for range 20 {
		imageCacheJob(t, s, 4<<10)
		s.imageStore.mu.Lock()
		s.imageStore.prune(time.Now(), 8<<10)
		s.imageStore.armCleanup()
		s.imageStore.mu.Unlock()
	}
	readers.Wait()
}

func assertImageCacheReply(t *testing.T, s *Server, result map[string]any, size int, download bool) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"result": result})
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	w.Write(body)
	m := readImageReply(t, w)
	if m.Result.IsError || len(m.Result.Content) != 2 {
		t.Fatalf("image result changed: %+v", m)
	}
	want := make([]byte, size)
	copy(want, pngBytes)
	got, err := base64.StdEncoding.DecodeString(m.Result.Content[1].Data)
	if err != nil || !bytes.Equal(got, want) {
		t.Fatal("inline image bytes changed")
	}
	if !bytes.Contains(m.Result.Meta[FilesMeta], []byte(`"inline":true`)) {
		t.Fatal("file metadata no longer describes an inline image")
	}
	if download {
		urls := urlsIn(m.Result.Content[0].Text)
		if len(urls) != 1 {
			t.Fatalf("image URLs: %v", urls)
		}
		mux := http.NewServeMux()
		mux.HandleFunc(ImageMCPFiles+"{name}", s.imageMCPFile)
		w = imageGet(mux, "127.0.0.1:5000", urls[0])
		if w.Code != http.StatusOK || !bytes.Equal(w.Body.Bytes(), want) {
			t.Fatalf("download changed: status %d, %d bytes", w.Code, w.Body.Len())
		}
	}
}

// Run with -benchtime=1x to measure the retained cache rather than temporary
// JSON and base64 allocations made while the images are being delivered.
func BenchmarkImageMCPJobsRetained(b *testing.B) {
	for range b.N {
		runtime.GC()
		var before, after runtime.MemStats
		runtime.ReadMemStats(&before)
		s := &Server{}
		for range 8 {
			imageCacheJob(b, s, 1<<20)
		}
		runtime.GC()
		runtime.ReadMemStats(&after)
		b.ReportMetric(float64(int64(after.HeapAlloc)-int64(before.HeapAlloc)), "retained-B")
		runtime.KeepAlive(s)
	}
}
