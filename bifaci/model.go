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

// ReceivedFlow is the order of one call's frames as a receiver takes them in,
// checked frame by frame with the proved model (formal/CapDAG/Bifaci/Request.lean,
// Emission.step; stepping is check, step_is_check): a stream started twice, a
// chunk or an end for a stream that is not open, a chunk_index other than the
// number of chunks received on its stream, a chunk_count that differs from it,
// END with a stream open, and any frame after END or ERR are each refused,
// naming what is wrong. Frames that are not of the flow are not passed to the
// model.
type ReceivedFlow struct {
	state model.Emission
}

// NewReceivedFlow is a call's flow before its first frame.
func NewReceivedFlow() *ReceivedFlow {
	return &ReceivedFlow{state: decided(model.EmissionStart())}
}

// Receive takes in the next frame of the call. On a violation the flow is
// broken: the error says what is wrong, and the state is left where the last
// frame in order put it.
func (r *ReceivedFlow) Receive(frame *Frame) error {
	if !frame.IsFlowFrame() {
		return nil
	}
	var emitted model.Emitted
	switch frame.FrameType {
	case FrameTypeStreamStart, FrameTypeChunk, FrameTypeStreamEnd:
		if frame.StreamId == nil {
			return fmt.Errorf("%s frame names no stream", frame.FrameType)
		}
		switch frame.FrameType {
		case FrameTypeStreamStart:
			emitted = model.EmittedStreamStart{Stream: *frame.StreamId}
		case FrameTypeChunk:
			if frame.ChunkIndex == nil {
				return fmt.Errorf("chunk on stream %s carries no chunk_index", *frame.StreamId)
			}
			emitted = model.EmittedChunk{Stream: *frame.StreamId, Index: nat(*frame.ChunkIndex)}
		default:
			emitted = model.EmittedStreamEnd{Stream: *frame.StreamId, Count: optionalNat(frame.ChunkCount)}
		}
	case FrameTypeEnd:
		emitted = model.EmittedFin{}
	case FrameTypeErr:
		emitted = model.EmittedErr{}
	default:
		emitted = model.EmittedOther{}
	}
	stepped := decided(model.Step(r.state, emitted))
	if !stepped.Ok {
		return fmt.Errorf("%s", describeViolation(stepped.Error))
	}
	r.state = stepped.Value
	return nil
}

func describeViolation(v model.Violation) string {
	switch v := v.(type) {
	case model.ViolationAfterEnd:
		return "a frame after the call's END or ERR"
	case model.ViolationStreamRestarted:
		return fmt.Sprintf("stream %s started twice", v.Stream)
	case model.ViolationStreamNotOpen:
		return fmt.Sprintf("a chunk or end for stream %s, which is not open", v.Stream)
	case model.ViolationChunkOutOfPlace:
		return fmt.Sprintf("chunk out of place on stream %s: expected %s, got %s", v.Stream, v.Expected, v.Got)
	case model.ViolationChunkCountWrong:
		return fmt.Sprintf("chunk count wrong on stream %s: sent %s, said %s", v.Stream, v.Sent, v.Said)
	case model.ViolationEndWithOpenStream:
		return fmt.Sprintf("END with stream %s still open", v.Stream)
	default:
		panic(fmt.Sprintf("BUG: the model's violation %#v has no description", v))
	}
}
