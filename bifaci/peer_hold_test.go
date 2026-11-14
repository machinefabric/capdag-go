package bifaci

// A peer request no master serves, held while the switch's provider brings
// the master that serves it (peer_hold.go, 14.2 §Peer requests no master
// serves).
//
// Every test runs a real switch over real connections: a caller master that
// makes a peer call, and — when the provider brings it — a second master
// serving the peer cap, attached to the running switch. The masters speak the
// wire protocol themselves, as this package's other switch tests do; the
// engine side drives the switch's inbound stream as a host's loop does.

import (
	"bytes"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// peerProvidedCap is the cap the caller's peer call names, served by no
// master at first.
const peerProvidedCap = `cap:in="media:enc=utf8";peer-provided;out="media:enc=utf8"`

// peerCallerCap is what the caller master advertises.
const peerCallerCap = `cap:in="media:enc=utf8";peer-caller;out="media:enc=utf8"`

// peerUnrelatedCap is a cap nothing asks for.
const peerUnrelatedCap = `cap:in="media:enc=utf8";peer-unrelated;out="media:enc=utf8"`

// peerRoomy is a bound no test's call comes near, but for its deadline.
var peerRoomy = PeerHoldBound{Frames: 64, Bytes: 1 << 20, Deadline: 10 * time.Second}

// peerCall is how the caller master makes its peer call.
type peerCall struct {
	argument []byte
	// cancel sends a CANCEL for the call right after its END.
	cancel bool
}

// peerCaller is a master whose cartridge makes peer calls: each call's
// answer is "ok:<the bytes the peer answered>", or "err:<CODE>:<message>"
// with the ERR the call ended with.
type peerCaller struct {
	writer  *FrameWriter
	answers chan string
	writeMu sync.Mutex
}

// startPeerCaller connects a caller master: it advertises peerCallerCap and
// answers every peer call's response into its answers channel.
func startPeerCaller(t *testing.T) (*peerCaller, SocketPair) {
	t.Helper()
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()
	caller := &peerCaller{writer: NewFrameWriter(slaveWrite), answers: make(chan string, 8)}
	go func() {
		reader := NewFrameReader(slaveRead)
		if !serveRelayHandshake(t, reader, caller.writer, []string{peerCallerCap}) {
			return
		}
		answers := map[string]*bytes.Buffer{}
		for {
			f, err := reader.ReadFrame()
			if err != nil || f == nil {
				return
			}
			rid := f.Id.ToString()
			switch f.FrameType {
			case FrameTypeChunk:
				if answers[rid] == nil {
					answers[rid] = &bytes.Buffer{}
				}
				answers[rid].Write(f.Payload)
			case FrameTypeEnd:
				answer := ""
				if answers[rid] != nil {
					answer = answers[rid].String()
				}
				delete(answers, rid)
				caller.answers <- "ok:" + answer
			case FrameTypeErr:
				delete(answers, rid)
				caller.answers <- fmt.Sprintf("err:%s:%s", f.ErrorCode(), f.ErrorMessage())
			}
		}
	}()
	return caller, SocketPair{ID: "caller-host", Read: engineRead, Write: engineWrite}
}

// call sends one peer call to peerProvidedCap, as a cartridge's peer invoker
// does: the REQ with no routing id, its argument stream, its END.
func (c *peerCaller) call(t *testing.T, call peerCall) MessageId {
	t.Helper()
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	rid := NewMessageIdRandom()
	streamID := "arg-0"
	frames := []*Frame{
		NewReq(rid, peerProvidedCap, []byte{}, "application/cbor"),
		NewStreamStart(rid, streamID, "media:enc=utf8", nil),
		NewChunk(rid, streamID, 0, call.argument, 0, ComputeChecksum(call.argument)),
		NewStreamEnd(rid, streamID, 1),
		NewEnd(rid, nil),
	}
	if call.cancel {
		frames = append(frames, NewCancelFrame(rid, UserCancelReason(false)))
	}
	for _, f := range frames {
		if err := c.writer.WriteFrame(f); err != nil {
			t.Fatalf("the caller's peer call could not be sent: %v", err)
		}
	}
	return rid
}

// answer is the next peer call's answer, or a failure after 30 s.
func (c *peerCaller) answer(t *testing.T) string {
	t.Helper()
	select {
	case a := <-c.answers:
		return a
	case <-time.After(30 * time.Second):
		t.Fatal("the peer call was not answered within 30 s")
		return ""
	}
}

// startUpper connects a master serving urn: it answers each request with
// its argument upper-cased, and counts the requests it served.
func startUpper(t *testing.T, id string, urn string, served *atomic.Int64) SocketPair {
	t.Helper()
	engineRead, slaveWrite := net.Pipe()
	slaveRead, engineWrite := net.Pipe()
	go func() {
		reader := NewFrameReader(slaveRead)
		writer := NewFrameWriter(slaveWrite)
		if !serveRelayHandshake(t, reader, writer, []string{urn}) {
			return
		}
		arguments := map[string]*bytes.Buffer{}
		for {
			f, err := reader.ReadFrame()
			if err != nil || f == nil {
				return
			}
			rid := f.Id.ToString()
			switch f.FrameType {
			case FrameTypeReq:
				arguments[rid] = &bytes.Buffer{}
			case FrameTypeChunk:
				arguments[rid].Write(f.Payload)
			case FrameTypeEnd:
				served.Add(1)
				answer := bytes.ToUpper(arguments[rid].Bytes())
				delete(arguments, rid)
				streamID := "answer"
				for _, out := range []*Frame{
					NewStreamStart(f.Id, streamID, "media:enc=utf8", nil),
					NewChunk(f.Id, streamID, 0, answer, 0, ComputeChecksum(answer)),
					NewStreamEnd(f.Id, streamID, 1),
					NewEnd(f.Id, nil),
				} {
					out.RoutingId = f.RoutingId
					if err := writer.WriteFrame(out); err != nil {
						return
					}
				}
			}
		}
	}()
	return SocketPair{ID: id, Read: engineRead, Write: engineWrite}
}

// testPeerProvider brings a master the way a host's provider brings a
// cartridge: it attaches one to the switch it was set on.
type testPeerProvider struct {
	t      *testing.T
	sw     *RelaySwitch
	asked  atomic.Int64
	served atomic.Int64
	// urn is the cap the master it brings serves, delay how long bringing it
	// takes; refusal, when set, is what it answers instead.
	urn     string
	delay   time.Duration
	refusal *PeerRefusal
}

func peerTestExpiry() PeerRefusal {
	return PeerRefusal{
		Code:    "TEST_PROVIDER_TIMEOUT",
		Class:   AttributionClassEnvironment,
		Message: "testcartridge, which provides peer-provided, was not started in time",
	}
}

func (p *testPeerProvider) Provision(capURN string) (*PeerProvision, *PeerRefusal) {
	if capURN != peerProvidedCap {
		p.t.Errorf("the provider is asked for the cap the peer call named, not %s", capURN)
	}
	p.asked.Add(1)
	if p.refusal != nil {
		refusal := *p.refusal
		return nil, &refusal
	}
	return &PeerProvision{
		Ready: func() *PeerRefusal {
			time.Sleep(p.delay)
			if _, err := p.sw.AddMaster(startUpper(p.t, "provided-host", p.urn, &p.served)); err != nil {
				return &PeerRefusal{Code: "TEST_ATTACH_FAILED", Class: AttributionClassInternal, Message: err.Error()}
			}
			return nil
		},
		OnExpiry: peerTestExpiry(),
	}, nil
}

// peerSwitch is a switch with one master — the caller — pumped as a host's
// loop pumps it, and, when given, provider as its peer provider within bound.
func peerSwitch(t *testing.T, provider *testPeerProvider, bound PeerHoldBound) (*RelaySwitch, *peerCaller) {
	t.Helper()
	caller, pair := startPeerCaller(t)
	sw, err := NewRelaySwitch([]SocketPair{pair}, HostPolicy{Network: NetworkPolicyFull})
	if err != nil {
		t.Fatalf("a switch with the caller must construct: %v", err)
	}
	if provider != nil {
		provider.t = t
		provider.sw = sw
		if err := sw.SetPeerProvider(provider, bound); err != nil {
			t.Fatalf("the first peer provider is taken: %v", err)
		}
	}
	sw.StartBackgroundPump()
	return sw, caller
}

// TEST463: A peer call to a cap no master serves is held while the switch's
// provider brings a master that serves it, and is then answered by that
// master, with every argument byte the caller sent while it was held.
func Test463_held_peer_call_is_answered_once_its_provider_is_brought(t *testing.T) {
	provider := &testPeerProvider{urn: peerProvidedCap, delay: 300 * time.Millisecond}
	_, caller := peerSwitch(t, provider, peerRoomy)

	caller.call(t, peerCall{argument: []byte("hello peer")})
	if got := caller.answer(t); got != "ok:HELLO PEER" {
		t.Fatalf("the held call is answered by the master brought: got %q", got)
	}
	if provider.asked.Load() != 1 || provider.served.Load() != 1 {
		t.Fatalf("asked %d, served %d; want 1 and 1", provider.asked.Load(), provider.served.Load())
	}

	// Brought once: the next call is routed to it at once.
	caller.call(t, peerCall{argument: []byte("again")})
	if got := caller.answer(t); got != "ok:AGAIN" {
		t.Fatalf("the next call is answered: got %q", got)
	}
	if provider.asked.Load() != 1 {
		t.Fatalf("a served cap is not provided again: asked %d times", provider.asked.Load())
	}
}

// TEST464: A provider that cannot bring what serves the cap ends the held
// call with its own ERR — its code and its message, which name what is
// missing and what to do.
func Test464_refused_provision_ends_the_call_with_the_providers_err(t *testing.T) {
	refusal := PeerRefusal{
		Code:  "CARTRIDGE_INSTALL_FAILED",
		Class: AttributionClassEnvironment,
		Message: "testcartridge provides peer-provided and is not installed; " +
			"install it with: capdag cartridge install testcartridge",
	}
	provider := &testPeerProvider{refusal: &refusal}
	_, caller := peerSwitch(t, provider, peerRoomy)

	caller.call(t, peerCall{argument: []byte("hello")})
	want := fmt.Sprintf("err:%s:%s", refusal.Code, refusal.Message)
	if got := caller.answer(t); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

// TEST465: A held call waits no longer than its deadline: it ends with the
// provider's own expiry ERR. The provider's work goes on, so what it brought
// late serves the next call.
func Test465_held_call_ends_at_its_deadline_and_the_late_provider_serves_the_next(t *testing.T) {
	bound := peerRoomy
	bound.Deadline = 300 * time.Millisecond
	provider := &testPeerProvider{urn: peerProvidedCap, delay: 1500 * time.Millisecond}
	_, caller := peerSwitch(t, provider, bound)

	started := time.Now()
	caller.call(t, peerCall{argument: []byte("hello")})
	expired := peerTestExpiry()
	if got := caller.answer(t); got != fmt.Sprintf("err:%s:%s", expired.Code, expired.Message) {
		t.Fatalf("the call ends with the provider's expiry ERR: got %q", got)
	}
	if time.Since(started) >= 1500*time.Millisecond {
		t.Fatal("the call ended at its deadline, not when the provider finished")
	}
	if provider.served.Load() != 0 {
		t.Fatal("the expired call was not replayed")
	}

	time.Sleep(1500 * time.Millisecond)
	caller.call(t, peerCall{argument: []byte("later")})
	if got := caller.answer(t); got != "ok:LATER" {
		t.Fatalf("the late provider serves the next call: got %q", got)
	}
	if provider.asked.Load() != 1 {
		t.Fatalf("asked %d times, want 1", provider.asked.Load())
	}
}

// TEST466: A hold keeps no more frames than its bound: a call that sends more
// while it is held ends with PEER_HOLD_OVERFLOW, and is never replayed.
func Test466_held_call_past_its_frame_bound_ends(t *testing.T) {
	// REQ, STREAM_START, CHUNK, STREAM_END and END are five frames.
	bound := peerRoomy
	bound.Frames = 4
	provider := &testPeerProvider{urn: peerProvidedCap, delay: 300 * time.Millisecond}
	_, caller := peerSwitch(t, provider, bound)

	caller.call(t, peerCall{argument: []byte("hello")})
	got := caller.answer(t)
	if !strings.HasPrefix(got, "err:"+PeerHoldOverflow+":") || !strings.Contains(got, "more than 4 frames") {
		t.Fatalf("got %q", got)
	}
	time.Sleep(600 * time.Millisecond)
	if provider.served.Load() != 0 {
		t.Fatal("the overflowed call was replayed")
	}
}

// TEST467: A hold keeps no more payload bytes than its bound.
func Test467_held_call_past_its_byte_bound_ends(t *testing.T) {
	bound := peerRoomy
	bound.Bytes = 1024
	provider := &testPeerProvider{urn: peerProvidedCap, delay: 300 * time.Millisecond}
	_, caller := peerSwitch(t, provider, bound)

	// 400 copies of "hello" is 2000 bytes, over the 1024 a hold keeps.
	caller.call(t, peerCall{argument: bytes.Repeat([]byte("hello"), 400)})
	got := caller.answer(t)
	if !strings.HasPrefix(got, "err:"+PeerHoldOverflow+":") || !strings.Contains(got, "1024 bytes") {
		t.Fatalf("got %q", got)
	}
	time.Sleep(600 * time.Millisecond)
	if provider.served.Load() != 0 {
		t.Fatal("the overflowed call was replayed")
	}
}

// TEST468: A caller that cancels its held call is answered with its cancel at
// once, and the call is not replayed to the provider brought after.
func Test468_cancelled_held_call_ends_at_once_and_is_not_replayed(t *testing.T) {
	provider := &testPeerProvider{urn: peerProvidedCap, delay: 800 * time.Millisecond}
	_, caller := peerSwitch(t, provider, peerRoomy)

	started := time.Now()
	caller.call(t, peerCall{argument: []byte("hello"), cancel: true})
	got := caller.answer(t)
	if !strings.HasPrefix(got, "err:CANCELLED:") {
		t.Fatalf("the cancelled call ends with its cancel: got %q", got)
	}
	if time.Since(started) >= 800*time.Millisecond {
		t.Fatal("the cancel was answered before the provider was there")
	}
	time.Sleep(1200 * time.Millisecond)
	if provider.served.Load() != 0 {
		t.Fatal("the cancelled call was replayed")
	}
}

// TEST469: A provider whose master does not serve the cap leaves the call
// held until its deadline, which ends it with the provider's expiry ERR; the
// provider is asked once.
func Test469_provider_that_serves_something_else_ends_the_call_at_its_deadline(t *testing.T) {
	bound := peerRoomy
	bound.Deadline = 800 * time.Millisecond
	provider := &testPeerProvider{urn: peerUnrelatedCap, delay: 50 * time.Millisecond}
	_, caller := peerSwitch(t, provider, bound)

	caller.call(t, peerCall{argument: []byte("hello")})
	expired := peerTestExpiry()
	if got := caller.answer(t); got != fmt.Sprintf("err:%s:%s", expired.Code, expired.Message) {
		t.Fatalf("got %q", got)
	}
	if provider.asked.Load() != 1 {
		t.Fatalf("asked %d times, want 1", provider.asked.Load())
	}
}

// TEST470: A switch without a provider refuses a peer call to a cap no master
// serves at once, with NO_HANDLER naming the cap.
func Test470_without_a_provider_the_call_is_refused_with_no_handler(t *testing.T) {
	_, caller := peerSwitch(t, nil, peerRoomy)
	caller.call(t, peerCall{argument: []byte("hello")})
	got := caller.answer(t)
	if !strings.HasPrefix(got, "err:NO_HANDLER:No handler found for cap:") || !strings.Contains(got, "peer-provided") {
		t.Fatalf("got %q", got)
	}
}

// TEST471: A switch takes one peer provider.
func Test471_a_second_peer_provider_is_refused(t *testing.T) {
	expiry := peerTestExpiry()
	provider := &testPeerProvider{refusal: &expiry}
	sw, _ := peerSwitch(t, provider, peerRoomy)
	err := sw.SetPeerProvider(provider, peerRoomy)
	if err == nil || !strings.Contains(err.Error(), "already has a peer provider") {
		t.Fatalf("a second provider is refused: %v", err)
	}
}
