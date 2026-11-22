package bifaci

// The decisions of the wire protocol's state machines — flow control, the
// order of a flow's frames, a request's lifecycle, admission through
// concurrency pools — are code generated from the proved model in
// ../../formal (CapDAG/Bifaci). This file is where this package's own types
// meet the model's; everything else here keeps what the model has no use for:
// the keyed containers, the waiting and waking, the I/O.

import (
	"fmt"
	"math"
	"math/big"
	"sync"

	model "github.com/machinefabric/capdag-go/formal"
)

// decided is the model's answer, or a panic: a call into the generated program
// fails only when its runtime does, never on a value this package built.
func decided[T any](answer T, err error) T {
	if err != nil {
		panic(fmt.Sprintf("bifaci: the model could not decide: %v", err))
	}
	return answer
}

// nat is a count as the model takes it.
func nat(n uint64) *big.Int { return new(big.Int).SetUint64(n) }

// count is a number out of the model that was put in as a uint64, or counts
// something this process holds in memory.
func count(n *big.Int) uint64 {
	if !n.IsUint64() {
		panic(fmt.Sprintf("bifaci: the model's count %s does not fit a uint64", n))
	}
	return n.Uint64()
}

// saturated is a number out of the model that may have grown past a uint64
// (a credit window granted without bound): the largest uint64 then.
func saturated(n *big.Int) uint64 {
	if !n.IsUint64() {
		return math.MaxUint64
	}
	return n.Uint64()
}

// model is the model's frame type for this one; frameTypeFromModel is its
// exact inverse.
func (ft FrameType) model() model.FrameType {
	switch ft {
	case FrameTypeHello:
		return model.FrameTypeHello{}
	case FrameTypeReq:
		return model.FrameTypeReq{}
	case FrameTypeChunk:
		return model.FrameTypeChunk{}
	case FrameTypeEnd:
		return model.FrameTypeFin{}
	case FrameTypeLog:
		return model.FrameTypeLog{}
	case FrameTypeErr:
		return model.FrameTypeErr{}
	case FrameTypeHeartbeat:
		return model.FrameTypeHeartbeat{}
	case FrameTypeStreamStart:
		return model.FrameTypeStreamStart{}
	case FrameTypeStreamEnd:
		return model.FrameTypeStreamEnd{}
	case FrameTypeRelayNotify:
		return model.FrameTypeRelayNotify{}
	case FrameTypeRelayState:
		return model.FrameTypeRelayState{}
	case FrameTypeCancel:
		return model.FrameTypeCancel{}
	case FrameTypeCredit:
		return model.FrameTypeCredit{}
	case FrameTypeCloseStream:
		return model.FrameTypeCloseStream{}
	case FrameTypePolicy:
		return model.FrameTypePolicy{}
	default:
		panic(fmt.Sprintf("BUG: FrameType %d has no model type", uint8(ft)))
	}
}

// frameTypeFromModel is the frame type the model names, the exact inverse of
// model.
func frameTypeFromModel(m model.FrameType) FrameType {
	switch m.(type) {
	case model.FrameTypeHello:
		return FrameTypeHello
	case model.FrameTypeReq:
		return FrameTypeReq
	case model.FrameTypeChunk:
		return FrameTypeChunk
	case model.FrameTypeFin:
		return FrameTypeEnd
	case model.FrameTypeLog:
		return FrameTypeLog
	case model.FrameTypeErr:
		return FrameTypeErr
	case model.FrameTypeHeartbeat:
		return FrameTypeHeartbeat
	case model.FrameTypeStreamStart:
		return FrameTypeStreamStart
	case model.FrameTypeStreamEnd:
		return FrameTypeStreamEnd
	case model.FrameTypeRelayNotify:
		return FrameTypeRelayNotify
	case model.FrameTypeRelayState:
		return FrameTypeRelayState
	case model.FrameTypeCancel:
		return FrameTypeCancel
	case model.FrameTypeCredit:
		return FrameTypeCredit
	case model.FrameTypeCloseStream:
		return FrameTypeCloseStream
	case model.FrameTypePolicy:
		return FrameTypePolicy
	default:
		panic(fmt.Sprintf("BUG: the model's frame type %#v has no runtime type", m))
	}
}

// FrameTypeOfCode is the frame type a wire number names, by the model's
// FrameType.ofCode; false for a number that names none.
func FrameTypeOfCode(code uint64) (FrameType, bool) {
	named := decided(model.OfCode(nat(code)))
	if !named.Valid {
		return 0, false
	}
	return frameTypeFromModel(named.Value), true
}

// Code is this frame type's wire number, by the model's FrameType.code.
func (ft FrameType) Code() uint64 { return ft.facts().code }

// frameFacts is what the model says of one frame type.
type frameFacts struct {
	flow     bool
	terminal bool
	code     uint64
}

// frameFactsByType is the model's answers for every frame type, asked once:
// they are asked of every frame that moves.
var frameFactsByType = sync.OnceValue(func() map[FrameType]frameFacts {
	facts := make(map[FrameType]frameFacts, len(FrameTypeAll))
	for _, ft := range FrameTypeAll {
		facts[ft] = frameFacts{
			flow:     decided(model.IsFlow(ft.model())),
			terminal: decided(model.IsTerminal(ft.model())),
			code:     count(decided(model.Code(ft.model()))),
		}
	}
	return facts
})

func (ft FrameType) facts() frameFacts {
	facts, ok := frameFactsByType()[ft]
	if !ok {
		panic(fmt.Sprintf("BUG: FrameType %d has no model type", uint8(ft)))
	}
	return facts
}

// IsFlow reports whether frames of this type are part of a request's flow:
// numbered in order, reordered at a relay boundary, and gated behind the
// flow's end. HELLO, HEARTBEAT, the relay frames, CANCEL, CREDIT and
// CLOSE_STREAM are not — CREDIT in particular must never wait behind a gap in
// the flow it is unblocking.
func (ft FrameType) IsFlow() bool { return ft.facts().flow }

// IsTerminal reports whether a frame of this type ends its flow: END or ERR.
func (ft FrameType) IsTerminal() bool { return ft.facts().terminal }
