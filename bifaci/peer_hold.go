package bifaci

// A peer request held while its provider is brought (14.2 §Peer requests no
// master serves).
//
// A cartridge's peer request names a cap. When no master behind the switch
// serves it, a switch that has a PeerProvider holds the request instead of
// refusing it: the REQ and every frame the caller sends for it after are kept,
// in the order they came, while the provider finds the cartridge that serves
// the cap, installs it if it is not installed, and hosts it. When a master
// serves the cap, the held frames are replayed in that order; when the
// provider cannot be brought, or not in time, the request ends with the ERR
// the provider words, naming the cartridge and what to do.
//
// Every decision about a held request — whether to hold it, what each frame
// does to the hold, how the hold ends — is the proved model's
// (formal/CapDAG/Bifaci/Hold.lean). This file keeps the held frames and asks
// it.

import (
	"fmt"
	"math/big"
	"time"

	model "github.com/machinefabric/capdag-go/formal"
)

// PeerHoldOverflow is the ERR code of a held request that sent more than its
// hold keeps.
const PeerHoldOverflow = "PEER_HOLD_OVERFLOW"

// PeerProviderTimeout is the ERR code of a held request whose provider was
// not found in time, before it could say what it was bringing.
const PeerProviderTimeout = "PEER_PROVIDER_TIMEOUT"

// PeerProviderFailed is the ERR code of a held request whose provider broke
// its contract.
const PeerProviderFailed = "PEER_PROVIDER_FAILED"

// PeerRefusal is why a held request ends without its provider: the ERR its
// caller is answered with, in the vocabulary of every ERR
// (docs/failure-taxonomy.md). The message is for the person who reads the
// run's failure: it names what is missing and the command that brings it.
type PeerRefusal struct {
	Code    string
	Class   AttributionClass
	Message string
}

// errFrame is the ERR answering request rid (routing id xid) with this
// refusal.
func (r PeerRefusal) errFrame(rid MessageId, xid MessageId) *Frame {
	frame := NewErr(rid, r.Code, r.Class, r.Message, nil)
	frame.RoutingId = &xid
	return frame
}

// PeerProvision is bringing a provider, once PeerProvider.Provision has
// found it.
type PeerProvision struct {
	// Ready brings it, and returns nil when a master behind the switch serves
	// the cap: the cartridge is installed and its host attached. It runs to
	// its end even when the hold it was started for does not wait that long,
	// so a provider that took longer than one request could wait is there for
	// the next.
	Ready func() *PeerRefusal
	// OnExpiry is what the caller is told when the hold's deadline passes
	// before Ready is done: the provider names the cartridge and the command
	// that installs it outside a run.
	OnExpiry PeerRefusal
}

// PeerProvider brings the master that serves a cap no master serves yet.
type PeerProvider interface {
	// Provision finds what provides capURN — the cap a cartridge's peer
	// request named — and returns how to bring it; or says why nothing can.
	// Exactly one of the two is non-nil.
	Provision(capURN string) (*PeerProvision, *PeerRefusal)
}

// PeerHoldBound is how much a hold may keep, and for how long.
type PeerHoldBound struct {
	// Frames is how many frames a hold keeps, the REQ among them.
	Frames uint64
	// Bytes is how many payload bytes a hold keeps.
	Bytes uint64
	// Deadline is how long a request is held before it ends with the
	// provider's OnExpiry, or a refusal saying nothing was found in time.
	Deadline time.Duration
}

func (b PeerHoldBound) model() model.Bound {
	return model.Bound{Frames: nat(b.Frames), Bytes: nat(b.Bytes)}
}

// peerHolds is the model's word on a peer request no master serves: whether
// it is held.
func peerHolds(canProvide bool, replayed bool) bool {
	switch decided(model.WhenUnrouted(canProvide, replayed)).(type) {
	case model.UnroutedHold:
		return true
	case model.UnroutedRefuse:
		return false
	default:
		panic("BUG: the model's Unrouted has two constructors")
	}
}

// heldRequest is one held request: who asked, the routing id its answers
// carry, its cap, and its frames in the order they came — the REQ first.
type heldRequest struct {
	sourceIdx int
	xid       MessageId
	capURN    string
	frames    []*Frame
	held      model.Held
}

// rid is the request's id.
func (r *heldRequest) rid() MessageId { return r.frames[0].Id }

// peerSettlement is how bringing a held request's provider ended. refusal is
// set for a refused or expired one.
type peerSettlement struct {
	bringing model.Bringing
	refusal  *PeerRefusal
}

func peerProvided() peerSettlement {
	return peerSettlement{bringing: model.BringingProvided{}}
}

func peerRefused(refusal PeerRefusal) peerSettlement {
	return peerSettlement{bringing: model.BringingRefused{}, refusal: &refusal}
}

func peerExpired(refusal PeerRefusal) peerSettlement {
	return peerSettlement{bringing: model.BringingExpired{}, refusal: &refusal}
}

// peerHoldTable is the held requests, by request id. Guarded by the
// switch's mu.
type peerHoldTable struct {
	bound PeerHoldBound
	held  map[string]*heldRequest
}

func newPeerHoldTable(bound PeerHoldBound) *peerHoldTable {
	return &peerHoldTable{bound: bound, held: map[string]*heldRequest{}}
}

func payloadSize(frame *Frame) *big.Int { return nat(uint64(len(frame.Payload))) }

// hold holds req — a peer REQ from master sourceIdx, already given its
// routing id. It returns the ERR the request ends with instead when the REQ
// alone is more than a hold may keep, else nil.
func (t *peerHoldTable) hold(sourceIdx int, req *Frame) *Frame {
	if req.RoutingId == nil || req.Cap == nil {
		panic("BUG: a held REQ names its cap and has been given its routing id")
	}
	switch arrival := decided(model.HoldArrive(t.bound.model(), decided(model.Initial()), FrameTypeReq.model(), payloadSize(req))).(type) {
	case model.HoldArrivalKeep:
		t.held[req.Id.ToString()] = &heldRequest{
			sourceIdx: sourceIdx,
			xid:       *req.RoutingId,
			capURN:    *req.Cap,
			frames:    []*Frame{req},
			held:      arrival.Held,
		}
		return nil
	case model.HoldArrivalOverflow:
		return t.overflowRefusal(*req.Cap).errFrame(req.Id, *req.RoutingId)
	default:
		panic("BUG: the model cancels only on a CANCEL frame, and this is a REQ")
	}
}

// isHeld reports whether rid from master sourceIdx is a held request.
func (t *peerHoldTable) isHeld(sourceIdx int, rid MessageId) bool {
	request, ok := t.held[rid.ToString()]
	return ok && request.sourceIdx == sourceIdx
}

// arrive takes a frame the caller sent for a held request: kept, and nil
// returned; or the request ends, and the ERR its caller is answered with is
// returned.
func (t *peerHoldTable) arrive(frame *Frame) *Frame {
	key := frame.Id.ToString()
	request, ok := t.held[key]
	if !ok {
		panic("BUG: arrive is asked only of a held request")
	}
	switch arrival := decided(model.HoldArrive(t.bound.model(), request.held, frame.FrameType.model(), payloadSize(frame))).(type) {
	case model.HoldArrivalKeep:
		request.held = arrival.Held
		request.frames = append(request.frames, frame)
		return nil
	case model.HoldArrivalOverflow:
		delete(t.held, key)
		return t.overflowRefusal(request.capURN).errFrame(frame.Id, request.xid)
	case model.HoldArrivalCancel:
		delete(t.held, key)
		reason, _ := frame.CancelReason()
		err := NewErr(frame.Id, reason.TerminalCode(), reason.TerminalClass(), reason.TerminalMessage(), nil)
		err.RoutingId = &request.xid
		return err
	default:
		panic("BUG: the model's Arrival has three constructors")
	}
}

// settle is the end of bringing the provider of held request rid. It returns
// the request and, when it is not to be replayed, the ERR its caller is
// answered with; nil when it is no longer held: it was cancelled, it
// overflowed, or its caller's master died.
func (t *peerHoldTable) settle(rid MessageId, settlement peerSettlement) (*heldRequest, *Frame) {
	key := rid.ToString()
	request, ok := t.held[key]
	if !ok {
		return nil, nil
	}
	delete(t.held, key)
	switch decided(model.FateOf(settlement.bringing)).(type) {
	case model.FateReplay:
		return request, nil
	case model.FateFail:
		if settlement.refusal == nil {
			panic("BUG: the model fails only a hold that was refused or expired")
		}
		return request, settlement.refusal.errFrame(rid, request.xid)
	default:
		panic("BUG: the model's Fate has two constructors")
	}
}

// forgetSource drops every request master sourceIdx asked for: it is gone,
// and so are they — nobody is left to answer.
func (t *peerHoldTable) forgetSource(sourceIdx int) {
	for key, request := range t.held {
		if request.sourceIdx == sourceIdx {
			delete(t.held, key)
		}
	}
}

func (t *peerHoldTable) overflowRefusal(capURN string) PeerRefusal {
	return PeerRefusal{
		Code:  PeerHoldOverflow,
		Class: AttributionClassResource,
		Message: fmt.Sprintf(
			"the peer call to %s sent more than %d frames or %d bytes while the "+
				"cartridge that provides it was being installed",
			capURN, t.bound.Frames, t.bound.Bytes,
		),
	}
}

// bringProvider brings the provider of capURN for a held request, within
// deadline: find it, wait for it to be ready, then for a master to serve the
// cap. Bringing it goes on past the deadline — only the wait ends. Called
// without sw.mu held.
func (sw *RelaySwitch) bringProvider(provider PeerProvider, capURN string, deadline time.Duration) peerSettlement {
	expiry := time.NewTimer(deadline)
	defer expiry.Stop()

	type found struct {
		provision *PeerProvision
		refusal   *PeerRefusal
	}
	finding := make(chan found, 1)
	go func() {
		provision, refusal := provider.Provision(capURN)
		finding <- found{provision, refusal}
	}()
	var provision *PeerProvision
	select {
	case <-expiry.C:
		return peerExpired(PeerRefusal{
			Code:    PeerProviderTimeout,
			Class:   AttributionClassEnvironment,
			Message: fmt.Sprintf("no cartridge providing %s was found within %d s", capURN, int(deadline.Seconds())),
		})
	case f := <-finding:
		switch {
		case f.refusal != nil && f.provision == nil:
			return peerRefused(*f.refusal)
		case f.provision != nil && f.refusal == nil && f.provision.Ready != nil:
			provision = f.provision
		default:
			return peerRefused(PeerRefusal{
				Code:    PeerProviderFailed,
				Class:   AttributionClassInternal,
				Message: fmt.Sprintf("the provider of %s answered neither a provision with its Ready nor a refusal alone", capURN),
			})
		}
	}

	ready := make(chan *PeerRefusal, 1)
	go func() { ready <- provision.Ready() }()
	select {
	case <-expiry.C:
		return peerExpired(provision.OnExpiry)
	case refusal := <-ready:
		if refusal != nil {
			return peerRefused(*refusal)
		}
	}

	poll := time.NewTicker(10 * time.Millisecond)
	defer poll.Stop()
	for {
		sw.mu.Lock()
		_, err := sw.findMasterForCap(capURN, nil)
		sw.mu.Unlock()
		if err == nil {
			return peerProvided()
		}
		select {
		case <-expiry.C:
			return peerExpired(provision.OnExpiry)
		case <-poll.C:
		}
	}
}
