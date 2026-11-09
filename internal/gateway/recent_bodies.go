package gateway

import (
	"log"

	"github.com/klauspost/compress/s2"
)

// Bodies are diagnostic copies, bounded by callBodyLimit. Compress the larger
// ones while they rest in the ring; small or incompressible bodies stay plain.
// The strings own exactly their encoded length, not S2's worst-case buffer.
func packRecentBody(body string) (plain, packed string) {
	if len(body) <= 4<<10 {
		return body, ""
	}
	encoded := s2.Encode(nil, []byte(body))
	if len(encoded) >= len(body) {
		return body, ""
	}
	return "", string(encoded)
}

func (c Call) packBodies() Call {
	c.RequestBody, c.packedRequest = packRecentBody(c.RequestBody)
	c.ResponseBody, c.packedResponse = packRecentBody(c.ResponseBody)
	return c
}

// unpackBodies reads only the call being opened, outside the server's lock.
// Compressed bytes are private, immutable and produced by packRecentBody.
func (c Call) unpackBodies() (Call, bool) {
	for _, body := range []struct {
		plain  *string
		packed string
	}{
		{&c.RequestBody, c.packedRequest},
		{&c.ResponseBody, c.packedResponse},
	} {
		if body.packed == "" {
			continue
		}
		decoded, err := s2.Decode(nil, []byte(body.packed))
		if err != nil {
			log.Printf("recent calls: call %d's body could not be read: %v", c.ID, err)
			return Call{}, false
		}
		*body.plain = string(decoded)
	}
	c.packedRequest, c.packedResponse = "", ""
	return c, true
}

// RecentSummaries lists the last calls without reading or retaining their
// bodies. The page polls these; only an opened row needs the full call.
func (s *Server) RecentSummaries() []Call {
	calls := s.recentSnapshot()
	for i := range calls {
		c := &calls[i]
		c.RequestBody, c.ResponseBody, c.packedRequest, c.packedResponse = "", "", "", ""
	}
	return calls
}

// recentSnapshot copies the ring newest first so bodies can be decoded after
// the lock has been released and after a concurrent record has evicted a call.
func (s *Server) recentSnapshot() []Call {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]Call, len(s.recent))
	for i, c := range s.recent {
		out[len(s.recent)-1-i] = c
	}
	return out
}
