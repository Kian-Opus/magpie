package gateway

import (
	"net/url"
	"strings"
	"time"
	"weak"
)

// linkJob binds a completed result to its files. Its inline image bytes share
// the files' storage, so evicting a file must also evict the cached result.
// Already waiting callers keep their result until delivery has finished.
// All store helpers below run under st.mu.
func (st *imageMCPStore) linkJob(j *imageJob) {
	meta, _ := j.result["_meta"].(map[string]any)
	files, _ := meta[FilesMeta].([]MadeFile)
	for _, file := range files {
		u, err := url.Parse(file.URL)
		if err != nil {
			delete(st.jobs, j.id)
			continue
		}
		name := u.Path[strings.LastIndex(u.Path, "/")+1:]
		if f := st.files[name]; f != nil {
			f.job = j.id
		} else {
			// A concurrent drawing may already have evicted this file.
			// Deliver this answer, but don't retain its bytes outside the budget.
			delete(st.jobs, j.id)
		}
	}
}

func (st *imageMCPStore) dropFile(name string) {
	if f := st.files[name]; f != nil {
		st.bytes -= len(f.data)
		delete(st.jobs, f.job)
		delete(st.files, name)
	}
}

// prune releases expired entries and applies the file budget, oldest first.
// As before, the newest file is kept even if it alone exceeds the budget.
func (st *imageMCPStore) prune(now time.Time, most int) {
	kept := st.order[:0]
	for _, name := range st.order {
		if f := st.files[name]; f != nil && now.Sub(f.made) >= imageMCPKeep {
			st.dropFile(name)
		} else {
			kept = append(kept, name)
		}
	}
	clear(st.order[len(kept):])
	st.order = kept
	for len(st.order) > 1 && st.bytes > most {
		st.dropFile(st.order[0])
		st.order[0] = ""
		st.order = st.order[1:]
	}
	for id, j := range st.jobs {
		select {
		case <-j.done:
			if now.Sub(j.started) >= imageMCPKeep {
				delete(st.jobs, id)
			}
		default:
		}
	}
	if len(st.order) == 0 {
		st.order = nil
	}
	if len(st.files) == 0 {
		st.files = nil
	}
	if len(st.jobs) == 0 {
		st.jobs = nil
	}
}

// armCleanup schedules the earliest expiry, without a polling goroutine. The
// timer only holds a weak reference: it must not keep a discarded Server and
// its image cache alive for an hour after that server has stopped being used.
func (st *imageMCPStore) armCleanup() {
	var next time.Time
	keep := func(at time.Time) {
		at = at.Add(imageMCPKeep)
		if next.IsZero() || at.Before(next) {
			next = at
		}
	}
	for _, f := range st.files {
		keep(f.made)
	}
	for _, j := range st.jobs {
		select {
		case <-j.done:
			keep(j.started)
		default:
		}
	}
	if next.IsZero() {
		if st.cleanup != nil {
			st.cleanup.Stop()
		}
		return
	}
	if st.cleanup != nil {
		st.cleanup.Reset(time.Until(next))
		return
	}
	ref := weak.Make(st)
	st.cleanup = time.AfterFunc(time.Until(next), func() {
		if st := ref.Value(); st != nil {
			st.mu.Lock()
			defer st.mu.Unlock()
			st.prune(time.Now(), imageMCPFilesMost)
			st.armCleanup()
		}
	})
}
