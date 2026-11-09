package gateway

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
)

var errBodyRoomSpool = errors.New("decoded body needs to wait for room")

// roomBodyReader reserves decoded bytes before reading them. The allowance is
// small so a large Read buffer does not reserve far beyond the arriving bytes.
// Like bodyRoom itself, this counts retained body bytes, not decoder windows,
// slice capacity, or garbage awaiting collection.
type roomBodyReader struct {
	ctx      context.Context
	r        io.Reader
	room     *bodyRoomT
	n        int64
	spooling bool
}

func (r *roomBodyReader) Read(p []byte) (int, error) {
	if err := r.ctx.Err(); err != nil {
		return 0, err
	}
	p = p[:min(len(p), 32<<10)]
	if !r.spooling && r.n < bodyRoomFree-1 {
		p = p[:min(int64(len(p)), bodyRoomFree-1-r.n)]
	}
	if !r.spooling && r.n == bodyRoomFree-1 {
		// Probe EOF before charging a body just below the exemption. Only
		// this one byte may cross the exemption before reserving its room.
		n, err := r.r.Read(p[:min(len(p), 1)])
		r.n += int64(n)
		if err != nil && err != io.EOF {
			return n, err
		}
		if n > 0 && !r.room.tryTake(r.ctx, r.n) {
			return n, errBodyRoomSpool
		}
		return n, err
	}
	if !r.spooling && !r.room.tryTake(r.ctx, r.n+int64(len(p))) {
		return 0, errBodyRoomSpool
	}
	n, err := r.r.Read(p)
	r.n += int64(n)
	return n, err
}

// readRoomBody keeps ordinary bodies in memory. When a growing body cannot
// fit alongside other requests, it moves to a private temporary file, releases
// its partial reservation, and waits for its full decoded size before loading
// it into memory. No waiter holds a partial body while asking for more room.
// The reader supplied by the caller is already limited to limit+1 bytes.
func readRoomBody(ctx context.Context, rd io.Reader, limit int64, beforeWait func()) (body []byte, status int, err error) {
	room := bodyRoom
	if ctx.Done() == nil {
		body, err = readSized(rd, -1)
		return body, 0, err
	}
	defer func() {
		if err != nil {
			room.give(ctx)
		}
	}()
	r := &roomBodyReader{ctx: ctx, r: rd, room: room}
	body, err = readSized(r, -1)
	if !errors.Is(err, errBodyRoomSpool) {
		if err == nil {
			room.trim(ctx, int64(len(body)))
		}
		return body, 0, err
	}
	f, err := os.CreateTemp("", "magpie-request-body-*")
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("could not spool request body: %w", err)
	}
	defer func() {
		f.Close()
		os.Remove(f.Name())
	}()
	if _, err = f.Write(body); err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("could not spool request body: %w", err)
	}
	body = nil
	room.give(ctx)
	r.spooling = true
	if _, err = io.CopyBuffer(f, r, make([]byte, 32<<10)); err != nil {
		// Decoder and socket errors keep their usual 400/408 status. File
		// write errors are server failures, not malformed client input.
		var fileError *os.PathError
		if errors.As(err, &fileError) {
			return nil, http.StatusInternalServerError, fmt.Errorf("could not spool request body: %w", err)
		}
		return nil, 0, err
	}
	if r.n > limit {
		return nil, http.StatusRequestEntityTooLarge, overLimit(limit)
	}
	if beforeWait != nil {
		beforeWait()
	}
	if err = room.take(ctx, r.n); err != nil {
		return nil, http.StatusServiceUnavailable, err
	}
	if _, err = f.Seek(0, io.SeekStart); err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("could not load request body: %w", err)
	}
	body, err = readSized(f, r.n)
	if err == nil && int64(len(body)) != r.n {
		err = io.ErrUnexpectedEOF
	}
	if err != nil {
		return nil, http.StatusInternalServerError, fmt.Errorf("could not load request body: %w", err)
	}
	return body, 0, nil
}
