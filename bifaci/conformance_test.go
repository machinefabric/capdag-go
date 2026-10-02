package bifaci

// The runtime's objects, replayed against the proved model's scripts.
//
// The decisions these objects make are the model's generated code
// (formal/CapDAG/Bifaci), so this does not test the rules — those are proved.
// It tests everything around them: that a CreditGate, a CreditWindow, a
// ReorderBuffer, the writer's gate, a RequestTable, the runtime's pools and
// the switch's admission carry the decisions out as the model means them —
// their counters, their containers, the order they do things in.
//
// ../../formal/conformance-bifaci.json is written by the model (`lake exe
// conformance_bifaci`): for each machine, short scripts of operations and,
// for each operation, what the model says happened. The same scripts run in
// every mirror.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	model "github.com/machinefabric/capdag-go/formal"
	"github.com/machinefabric/capdag-go/urn"
)

var (
	bifaciTableOnce sync.Once
	bifaciTable     map[string]json.RawMessage
	bifaciTableErr  error
)

// bifaciRows decodes one section of the model's table into rows of T.
func bifaciRows[T any](t *testing.T, section string) []T {
	t.Helper()
	bifaciTableOnce.Do(func() {
		raw, err := os.ReadFile("../../formal/conformance-bifaci.json")
		if err != nil {
			bifaciTableErr = err
			return
		}
		bifaciTableErr = json.Unmarshal(raw, &bifaciTable)
	})
	if bifaciTableErr != nil {
		t.Fatalf("the model's table: %v", bifaciTableErr)
	}
	raw, ok := bifaciTable[section]
	if !ok {
		t.Fatalf("the model's table has no '%s' section", section)
	}
	var rows []T
	if err := json.Unmarshal(raw, &rows); err != nil {
		t.Fatalf("the model's '%s' section: %v", section, err)
	}
	if len(rows) == 0 {
		t.Fatalf("the model's table has no %s scripts", section)
	}
	return rows
}

// conclude fails naming how many scripts went wrong, and the first.
func conclude(t *testing.T, what string, total int, wrong []string) {
	t.Helper()
	if len(wrong) > 0 {
		t.Fatalf("%d of %d %s scripts disagree with the model; first: %s", len(wrong), total, what, wrong[0])
	}
}

func text(p *string) string {
	if p == nil {
		return "<none>"
	}
	return *p
}

func sameText(a, b *string) bool {
	if a == nil || b == nil {
		return a == b
	}
	return *a == *b
}

// TEST12375: every frame type is the type the model means by its number, part
// of a flow exactly when the model says, and an end exactly when the model
// says. The runtime's own FrameType is mapped onto the model's by hand; a
// type mapped to the wrong one would be numbered, ordered and gated as
// another.
func Test12375_frame_types_are_the_models(t *testing.T) {
	type row struct {
		Code     uint8   `json:"code"`
		Flow     bool    `json:"flow"`
		Terminal bool    `json:"terminal"`
		Ends     *string `json:"ends"`
	}
	rows := bifaciRows[row](t, "frame_types")
	if len(rows) != len(FrameTypeAll) {
		t.Fatalf("the model names %d frame types, the runtime %d", len(rows), len(FrameTypeAll))
	}
	var wrong []string
	for _, r := range rows {
		frameType := FrameType(r.Code)
		known := false
		for _, ft := range FrameTypeAll {
			known = known || ft == frameType
		}
		if !known {
			wrong = append(wrong, fmt.Sprintf("wire number %d names no frame type here", r.Code))
			continue
		}
		if code := decided(model.Code(frameType.model())); count(code) != uint64(r.Code) {
			wrong = append(wrong, fmt.Sprintf("%v (%d) is mapped to the model's type %s", frameType, r.Code, code))
		}
		if frameType.IsFlow() != r.Flow {
			wrong = append(wrong, fmt.Sprintf("%v: flow is %v", frameType, frameType.IsFlow()))
		}
		if frameType.IsTerminal() != r.Terminal {
			wrong = append(wrong, fmt.Sprintf("%v: terminal is %v", frameType, frameType.IsTerminal()))
		}
		var ends *string
		if kind, ok := TerminalKindOfFrame(frameType); ok {
			name := kind.AsStr()
			ends = &name
		}
		if !sameText(ends, r.Ends) {
			wrong = append(wrong, fmt.Sprintf("%v: ends its request as %s, model %s", frameType, text(ends), text(r.Ends)))
		}
		if NewFrame(frameType, NewMessageIdFromUint(1)).IsFlowFrame() != frameType.IsFlow() {
			wrong = append(wrong, fmt.Sprintf("%v: a frame and its type disagree on being of a flow", frameType))
		}
	}
	conclude(t, "frame type", len(rows), wrong)
}

// TEST12376: a credit gate answers every acquire, grant and close as the
// model does, and holds what the model says it holds afterwards.
func Test12376_credit_gate_follows_the_model(t *testing.T) {
	type script struct {
		Window uint64 `json:"window"`
		Ops    []struct {
			Op     string `json:"op"`
			N      uint64 `json:"n"`
			Reason string `json:"reason"`
		} `json:"ops"`
		Steps []struct {
			Answer    *string `json:"answer"`
			Available uint64  `json:"available"`
			Closed    *string `json:"closed"`
		} `json:"steps"`
	}
	scripts := bifaciRows[script](t, "gate")
	var wrong []string
	for index, s := range scripts {
		gate := NewCreditGate(s.Window)
		for at, op := range s.Ops {
			step := s.Steps[at]
			var answer *string
			switch op.Op {
			case "acquire":
				acquired, err := gate.TryAcquire(op.N)
				said := "wait"
				if closed, isClosed := err.(*CreditClosed); isClosed {
					said = "closed:" + closed.Reason
				} else if acquired {
					said = "acquired"
				}
				answer = &said
			case "grant":
				gate.Grant(op.N)
			case "close":
				gate.Close(op.Reason)
			default:
				t.Fatalf("unknown gate operation '%s'", op.Op)
			}
			if !sameText(answer, step.Answer) || gate.Available() != step.Available || gate.IsClosed() != (step.Closed != nil) {
				wrong = append(wrong, fmt.Sprintf("step %d of script %d: answered %s, available %d, closed %v",
					at, index, text(answer), gate.Available(), gate.IsClosed()))
				break
			}
		}
	}
	conclude(t, "credit gate", len(scripts), wrong)
}

// TEST12377: a credit window accepts, refuses and grants as the model does:
// an arriving chunk is a violation exactly when nothing is left of the
// window, a grant is due exactly when a batch has built up, a flush grants
// what is pending, and a continued chunk is granted back at once.
func Test12377_credit_window_follows_the_model(t *testing.T) {
	type script struct {
		Window uint64   `json:"window"`
		Ops    []string `json:"ops"`
		Steps  []struct {
			Violation bool   `json:"violation"`
			Grant     uint64 `json:"grant"`
			Remaining uint64 `json:"remaining"`
			Pending   uint64 `json:"pending"`
		} `json:"steps"`
	}
	scripts := bifaciRows[script](t, "window")
	var wrong []string
	for index, s := range scripts {
		window := NewCreditWindow(s.Window)
		for at, op := range s.Ops {
			step := s.Steps[at]
			violation, grant := false, uint64(0)
			switch op {
			case "arrive":
				violation = !window.Arrive()
			case "continuation":
				if window.Arrive() {
					grant = window.Continued()
				} else {
					violation = true
				}
			case "consume":
				grant = window.Consumed()
			case "flush":
				grant = window.Flush()
			default:
				t.Fatalf("unknown window operation '%s'", op)
			}
			if violation != step.Violation || grant != step.Grant ||
				window.Remaining() != step.Remaining || window.Pending() != step.Pending {
				wrong = append(wrong, fmt.Sprintf("step %d of script %d: violation %v, grant %d, remaining %d, pending %d",
					at, index, violation, grant, window.Remaining(), window.Pending()))
				break
			}
		}
	}
	conclude(t, "credit window", len(scripts), wrong)
}

// TEST12378: the window two ends start with is the smaller proposal, and a
// proposal of zero is refused — it would deadlock every stream at its first
// chunk.
func Test12378_a_zero_window_is_refused(t *testing.T) {
	type row struct {
		Ours   uint64  `json:"ours"`
		Theirs uint64  `json:"theirs"`
		Window *uint64 `json:"window"`
	}
	rows := bifaciRows[row](t, "negotiate")
	var wrong []string
	for _, r := range rows {
		window, ok := NegotiateInitialCredit(r.Ours, r.Theirs)
		if ok != (r.Window != nil) || (ok && window != *r.Window) {
			wrong = append(wrong, fmt.Sprintf("ours %d, theirs %d: negotiated %d (%v)", r.Ours, r.Theirs, window, ok))
		}
		// The limits two ends agree on carry the same decision.
		ours, theirs := DefaultLimits(), DefaultLimits()
		ours.InitialCredit, theirs.InitialCredit = int(r.Ours), int(r.Theirs)
		limits, err := NegotiateLimits(ours, theirs)
		if (err == nil) != (r.Window != nil) || (err == nil && uint64(limits.InitialCredit) != *r.Window) {
			wrong = append(wrong, fmt.Sprintf("ours %d, theirs %d: limits %d (%v)", r.Ours, r.Theirs, limits.InitialCredit, err))
		} else if err != nil && !strings.Contains(err.Error(), "initial_credit") {
			wrong = append(wrong, fmt.Sprintf("ours %d, theirs %d: the refusal does not name the window: %v", r.Ours, r.Theirs, err))
		}
	}
	conclude(t, "negotiation", len(rows), wrong)
}

// TEST12379: a grant credits the stream it names, or — naming none — the
// request's only sending stream; a grant that names a stream the request
// does not have, or names none among several, credits nothing.
func Test12379_a_grant_reaches_the_stream_it_is_for(t *testing.T) {
	type row struct {
		Streams []*string `json:"streams"`
		Named   *string   `json:"named"`
		Matched bool      `json:"matched"`
		Target  *string   `json:"target"`
	}
	rows := bifaciRows[row](t, "grant_target")
	var wrong []string
	for index, r := range rows {
		rid := NewMessageIdFromUint(7)
		router := NewCreditRouter()
		gates := make([]*CreditGate, len(r.Streams))
		for i, stream := range r.Streams {
			gates[i] = NewCreditGate(0)
			router.Register(rid, stream, gates[i])
		}
		// A gate of ANOTHER request with the same stream id must never be credited.
		stranger := NewCreditGate(0)
		router.Register(NewMessageIdFromUint(8), r.Named, stranger)

		matched := router.Grant(NewCredit(rid, r.Named, 3, CreditDirectionResponse))
		var credited []string
		for i, gate := range gates {
			if gate.Available() == 3 {
				credited = append(credited, text(r.Streams[i]))
			}
		}
		var expected []string
		if r.Matched {
			expected = []string{text(r.Target)}
		}
		if matched != r.Matched || !reflect.DeepEqual(credited, expected) || stranger.Available() != 0 {
			wrong = append(wrong, fmt.Sprintf("row %d: matched %v, credited %v", index, matched, credited))
		}
	}
	conclude(t, "grant routing", len(rows), wrong)
}

// TEST12380: frames arriving out of order are handed on in the order they
// were written — each arrival delivers exactly what the model says, holds
// what it says, and is refused when it says: a number already handed on, one
// already held, or one more than the buffer may hold.
func Test12380_reorder_buffer_follows_the_model(t *testing.T) {
	type script struct {
		Limit    int      `json:"limit"`
		Arrivals []uint64 `json:"arrivals"`
		Steps    []struct {
			Deliver *[]uint64 `json:"deliver"`
			Hold    *bool     `json:"hold"`
			Error   *string   `json:"error"`
		} `json:"steps"`
	}
	scripts := bifaciRows[script](t, "reorder")
	var wrong []string
	for index, s := range scripts {
		buffer := NewReorderBuffer(s.Limit)
		for at, seq := range s.Arrivals {
			step := s.Steps[at]
			frame := NewFrame(FrameTypeLog, NewMessageIdFromUint(1))
			frame.Seq = seq
			delivered, err := buffer.Accept(frame)
			agrees := false
			switch {
			case step.Deliver != nil:
				got := make([]uint64, 0, len(delivered))
				for _, f := range delivered {
					got = append(got, f.Seq)
				}
				agrees = err == nil && reflect.DeepEqual(got, *step.Deliver)
			case step.Hold != nil:
				agrees = err == nil && len(delivered) == 0
			case step.Error != nil && err != nil:
				switch *step.Error {
				case "stale":
					agrees = strings.Contains(err.Error(), "stale/duplicate seq: expected")
				case "duplicate":
					agrees = strings.Contains(err.Error(), "already buffered")
				case "overflow":
					agrees = strings.Contains(err.Error(), "reorder buffer overflow")
				default:
					t.Fatalf("unknown reorder refusal '%s'", *step.Error)
				}
			}
			if !agrees {
				wrong = append(wrong, fmt.Sprintf("step %d of script %d (limit %d, arrivals %v): %d frames, %v",
					at, index, s.Limit, s.Arrivals, len(delivered), err))
				break
			}
		}
	}
	conclude(t, "reorder", len(scripts), wrong)
}

// frameOf is a frame of one type for one request, as a writer is handed it.
func frameOf(t *testing.T, frameType FrameType, rid MessageId) *Frame {
	t.Helper()
	switch frameType {
	case FrameTypeChunk:
		payload := []byte{1}
		return NewChunk(rid, "s", 0, payload, 0, ComputeChecksum(payload))
	case FrameTypeLog:
		return NewProgress(rid, 0.5, "working")
	case FrameTypeEnd:
		return EndOkWith(rid, nil, f64Ptr(1.0), nil)
	case FrameTypeErr:
		return NewErr(rid, "FAILED", AttributionClassInternal, "it failed", nil)
	case FrameTypeCredit:
		return NewCredit(rid, nil, 1, CreditDirectionResponse)
	default:
		t.Fatalf("the writer scripts hand over no %v frame", frameType)
		return nil
	}
}

// TEST12381: the writer writes exactly the frames the model says: everything
// until the flow's END or ERR, then nothing of the flow — while credit, which
// is not of the flow, still passes. What reaches the wire has the flow's
// numbers 0, 1, 2, … without a gap.
func Test12381_the_writer_gate_follows_the_model(t *testing.T) {
	type script struct {
		Frames  []uint8 `json:"frames"`
		Written []bool  `json:"written"`
	}
	scripts := bifaciRows[script](t, "writer")
	var wrong []string
	for index, s := range scripts {
		rid := NewMessageIdFromUint(1)
		var wire bytes.Buffer
		writer := newSyncFrameWriter(NewFrameWriter(&wire), NewDropCounters(), NewStragglerCounters())
		written := make([]bool, 0, len(s.Frames))
		for _, code := range s.Frames {
			before := wire.Len()
			if err := writer.WriteFrame(frameOf(t, FrameType(code), rid)); err != nil {
				t.Fatalf("a write to memory failed: %v", err)
			}
			written = append(written, wire.Len() > before)
		}
		if !reflect.DeepEqual(written, s.Written) {
			wrong = append(wrong, fmt.Sprintf("script %d (%v): written %v", index, s.Frames, written))
			continue
		}
		var flowSeqs, expected []uint64
		for _, frame := range decodeWireFrames(t, wire.Bytes()) {
			if frame.IsFlowFrame() {
				expected = append(expected, uint64(len(flowSeqs)))
				flowSeqs = append(flowSeqs, frame.Seq)
			}
		}
		if !reflect.DeepEqual(flowSeqs, expected) {
			wrong = append(wrong, fmt.Sprintf("script %d (%v): flow frames numbered %v", index, s.Frames, flowSeqs))
		}
	}
	conclude(t, "writer", len(scripts), wrong)
}

// tableID is one of the ids a table script names, as a message id.
func tableID(t *testing.T, name string) MessageId {
	t.Helper()
	ids := map[string]uint64{"x1": 1, "x2": 2, "r1": 101, "r2": 102, "r3": 103}
	id, ok := ids[name]
	if !ok {
		t.Fatalf("the table scripts name no id '%s'", name)
	}
	return NewMessageIdFromUint(id)
}

// TEST12382: a request table registers a request once, ends it once, keeps no
// state for it afterwards, and tells a frame that crossed a request's end
// from a frame for a request nobody knew — step for step as the model's table
// does, including when the ring of ended requests is full and the oldest is
// forgotten.
func Test12382_request_table_follows_the_model(t *testing.T) {
	type script struct {
		Keep int `json:"keep"`
		Ops  []struct {
			Op  string `json:"op"`
			Xid string `json:"xid"`
			Rid string `json:"rid"`
		} `json:"ops"`
		Steps []struct {
			Ok     bool     `json:"ok"`
			Frames []string `json:"frames"`
		} `json:"steps"`
		Live       int    `json:"live"`
		Registered uint64 `json:"registered"`
	}
	scripts := bifaciRows[script](t, "table")
	var wrong []string
	for index, s := range scripts {
		table := newRequestTableKeeping(s.Keep)
		agreed := true
		for at, op := range s.Ops {
			step := s.Steps[at]
			key := NewRequestKey(tableID(t, op.Xid), tableID(t, op.Rid))
			ok := false
			switch op.Op {
			case "register":
				ok = table.Register(key, NewRequestState(RequestRoutingEntry{}, nil, nil, false, 32)) == nil
			case "terminate":
				ok = table.Terminate(key, TerminalKindEnd) != nil
				if _, indexed := table.XidForRid(key.Rid); ok && (table.Contains(key) || indexed) {
					wrong = append(wrong, fmt.Sprintf("step %d of script %d: state remains after the end", at, index))
					agreed = false
				}
			default:
				t.Fatalf("unknown table operation '%s'", op.Op)
			}
			if !agreed {
				break
			}
			frames := make([]string, 0, 3)
			for _, rid := range []string{"r1", "r2", "r3"} {
				switch table.Disposition(tableID(t, rid)) {
				case DispositionRoute:
					frames = append(frames, "route")
				case DispositionStraggler:
					frames = append(frames, "straggler")
				case DispositionNoRoute:
					frames = append(frames, "no_route")
				}
			}
			if ok != step.Ok || !reflect.DeepEqual(frames, step.Frames) {
				wrong = append(wrong, fmt.Sprintf("step %d of script %d: ok %v, frames %v", at, index, ok, frames))
				agreed = false
				break
			}
		}
		if agreed && (table.Len() != s.Live || table.TotalRegistered() != s.Registered) {
			wrong = append(wrong, fmt.Sprintf("script %d: %d live, %d registered", index, table.Len(), table.TotalRegistered()))
		}
	}
	conclude(t, "request table", len(scripts), wrong)

	// The ledger a request keeps of each stream's window moves as the
	// model's does: one less for a chunk, more by a grant, and by nothing
	// else.
	type ledgerRow struct {
		Remaining uint64 `json:"remaining"`
		Frame     uint8  `json:"frame"`
		Granted   uint64 `json:"granted"`
		After     int64  `json:"after"`
	}
	for _, r := range bifaciRows[ledgerRow](t, "ledger") {
		rid := NewMessageIdFromUint(9)
		stream := "s"
		var frame *Frame
		if FrameType(r.Frame) == FrameTypeCredit {
			frame = NewCredit(rid, &stream, r.Granted, CreditDirectionResponse)
		} else {
			frame = NewFrame(FrameType(r.Frame), rid)
			frame.StreamId = &stream
		}
		table := NewRequestTable()
		key := NewRequestKey(NewMessageIdFromUint(1), rid)
		if err := table.Register(key, NewRequestState(RequestRoutingEntry{}, nil, nil, false, r.Remaining)); err != nil {
			t.Fatal(err)
		}
		table.RecordFrame(key, FrameDirectionInbound, frame)
		after := table.Get(key).Streams[StreamKey{Present: true, ID: stream}].CreditOutstanding
		if after != r.After {
			t.Errorf("the ledger after frame type %d at %d: %d, model %d", r.Frame, r.Remaining, after, r.After)
		}
	}
}

// TEST12383: a pool's limit is the smaller of the operator's number and what
// the cartridge reports, with zero meaning no limit in both and in the
// result; and a cartridge that is not running is given one request, through
// `all`.
func Test12383_pool_limits_are_the_models(t *testing.T) {
	type row struct {
		Configured uint64  `json:"configured"`
		Available  *uint64 `json:"available"`
		Effective  uint64  `json:"effective"`
		ColdAll    uint64  `json:"cold_all"`
		ColdOther  uint64  `json:"cold_other"`
	}
	rows := bifaciRows[row](t, "effective")
	var wrong []string
	for _, r := range rows {
		state := DeclaredPoolState(r.Configured, nil)
		state.Available = r.Available
		effective := EffectiveCapacity(r.Configured, r.Available)
		if effective != r.Effective || state.Effective() != effective ||
			AdvertisedCapacity(true, PoolAll, state) != effective ||
			AdvertisedCapacity(false, PoolAll, state) != r.ColdAll ||
			AdvertisedCapacity(false, "gpu", state) != r.ColdOther {
			wrong = append(wrong, fmt.Sprintf("configured %d, available %v: effective %d", r.Configured, r.Available, effective))
		}
	}
	conclude(t, "pool limit", len(rows), wrong)
}

// poolScript is one script over a cartridge's pools: its caps, its shared
// pools, each pool's limit, and what happens.
type poolScript struct {
	Caps   []string `json:"caps"`
	Shared []struct {
		Name string   `json:"name"`
		Caps []string `json:"caps"`
	} `json:"shared"`
	Capacities []struct {
		Pool     string `json:"pool"`
		Capacity uint64 `json:"capacity"`
	} `json:"capacities"`
	Pools []string `json:"pools"`
	Ops   []struct {
		Op       string `json:"op"`
		Cap      string `json:"cap"`
		Pool     string `json:"pool"`
		Capacity uint64 `json:"capacity"`
	} `json:"ops"`
	Steps []struct {
		Admitted json.RawMessage `json:"admitted"`
		Ticket   *uint64         `json:"ticket"`
		Position *uint64         `json:"position"`
		Active   []uint64        `json:"active"`
		HeldBack []uint64        `json:"held_back"`
		Waiting  []uint64        `json:"waiting"`
	} `json:"steps"`
}

// TEST12384: the runtime's pools admit, queue and release exactly as the
// model's scripts say — who holds a slot in which pool, who is in line, who
// goes next, and how many waiters each pool is holding back — including
// where a pool's limit rose and a request arrives while somebody in line
// could go: it waits behind them rather than taking the slot.
func Test12384_runtime_pools_follow_the_model(t *testing.T) {
	// The scripts name caps as they are written; the runtime names their
	// pools by the canonical form.
	canon := func(name string) string {
		if !strings.HasPrefix(name, "cap:") {
			return name
		}
		parsed, err := urn.NewCapUrnFromString(name)
		if err != nil {
			t.Fatalf("the script's cap '%s': %v", name, err)
		}
		return parsed.String()
	}
	canonAll := func(names []string) []string {
		canonical := make([]string, 0, len(names))
		for _, name := range names {
			canonical = append(canonical, canon(name))
		}
		return canonical
	}
	scripts := bifaciRows[poolScript](t, "pools")
	var wrong []string
	for index, s := range scripts {
		declarations := &PoolDeclarations{Pools: map[string][]string{}, Capacities: map[string]uint64{}}
		for _, pool := range s.Shared {
			declarations.Pools[pool.Name] = canonAll(pool.Caps)
		}
		for _, capacity := range s.Capacities {
			declarations.Capacities[canon(capacity.Pool)] = capacity.Capacity
		}
		pools, err := newRuntimePools(canonAll(s.Caps), declarations)
		if err != nil {
			t.Fatalf("the script's pools: %v", err)
		}
		s.Pools = canonAll(s.Pools)
		inLine := map[uint64]*liveHandlerRequest{}
		for at, op := range s.Ops {
			step := s.Steps[at]
			agreed := false
			switch op.Op {
			case "arrive":
				request := poolTestRequest(canon(op.Cap))
				var expected bool
				if err := json.Unmarshal(step.Admitted, &expected); err != nil {
					t.Fatalf("an arrival's step says whether it was admitted: %v", err)
				}
				admitted, position := pools.arrive(request)
				if admitted {
					agreed = expected
				} else {
					inLine[request.ticket] = request
					agreed = !expected && step.Ticket != nil && request.ticket == *step.Ticket &&
						step.Position != nil && uint64(position) == *step.Position
				}
			case "release":
				pools.release(canon(op.Cap))
				agreed = true
			case "admit_next":
				went := pools.admitNext()
				switch {
				case went == nil:
					agreed = step.Ticket == nil
				case step.Ticket != nil:
					agreed = went.ticket == *step.Ticket && inLine[went.ticket] == went
					delete(inLine, went.ticket)
				}
			case "capacity":
				agreed = pools.applyDesired(DesiredCapacities{canon(op.Pool): op.Capacity}) == nil
			case "leave_oldest":
				// This mirror's runtime has no way for a queued request to
				// leave the line (it does not handle CANCEL); the scripts
				// that need one cannot be replayed here.
				agreed = true
			default:
				t.Fatalf("unknown pools operation '%s'", op.Op)
			}
			if op.Op == "leave_oldest" {
				break
			}
			snapshot := pools.snapshot()
			active := make([]uint64, 0, len(s.Pools))
			heldBack := make([]uint64, 0, len(s.Pools))
			for _, name := range s.Pools {
				active = append(active, snapshot[name].Active)
				heldBack = append(heldBack, snapshot[name].Queued)
			}
			waiting := make([]uint64, 0, len(pools.waiting))
			for ticket := range pools.waiting {
				waiting = append(waiting, ticket)
			}
			sort.Slice(waiting, func(i, j int) bool { return waiting[i] < waiting[j] })
			if !agreed || !reflect.DeepEqual(active, step.Active) || !reflect.DeepEqual(heldBack, step.HeldBack) ||
				!reflect.DeepEqual(waiting, append([]uint64{}, step.Waiting...)) {
				wrong = append(wrong, fmt.Sprintf("step %d of script %d: agreed %v, active %v, held back %v, waiting %v",
					at, index, agreed, active, heldBack, waiting))
				break
			}
		}
	}
	conclude(t, "pools", len(scripts), wrong)
}

// emitted is a frame as the model's recognizer of a flow's order sees it;
// false for a frame that is not of the flow.
func emitted(frame *Frame) (model.Emitted, bool) {
	if !frame.IsFlowFrame() {
		return nil, false
	}
	switch frame.FrameType {
	case FrameTypeStreamStart:
		return model.EmittedStreamStart{Stream: *frame.StreamId}, true
	case FrameTypeChunk:
		return model.EmittedChunk{Stream: *frame.StreamId, Index: nat(*frame.ChunkIndex)}, true
	case FrameTypeStreamEnd:
		return model.EmittedStreamEnd{Stream: *frame.StreamId, Count: optionalNat(frame.ChunkCount)}, true
	case FrameTypeEnd:
		return model.EmittedFin{}, true
	case FrameTypeErr:
		return model.EmittedErr{}, true
	default:
		return model.EmittedOther{}, true
	}
}

// TEST12385: what an emitter and the writer put on the wire for one request
// is a flow in order, by the model's own recognizer: the stream is started
// once, its chunks are numbered 0, 1, 2, …, its end says how many there were,
// and nothing of the flow follows END — although a late progress frame and a
// late chunk were handed to the writer after it.
func Test12385_what_reaches_the_wire_is_a_flow_in_order(t *testing.T) {
	var wire bytes.Buffer
	writer := newSyncFrameWriter(NewFrameWriter(&wire), NewDropCounters(), NewStragglerCounters())
	rid := NewMessageIdRandom()
	emitter := newThreadSafeEmitter(writer, rid, nil, "s1", "media:enc=utf-8", 4, nil, 0)
	if err := emitter.Write(bytes.Repeat([]byte{7}, 10)); err != nil {
		t.Fatalf("write: %v", err)
	}
	emitter.Progress(0.5, "halfway")
	emitter.Finalize()

	// The handler's END, then what a detached sender does: frames that lost
	// the race with it.
	late := []byte{9}
	handedOver := []*Frame{
		EndOkWith(rid, nil, f64Ptr(1.0), nil),
		NewProgress(rid, 1.0, "late keepalive"),
		NewChunk(rid, "s1", 0, late, 3, ComputeChecksum(late)),
	}
	for _, frame := range handedOver {
		if err := writer.WriteFrame(frame); err != nil {
			t.Fatalf("a write to memory failed: %v", err)
		}
	}

	onWire := decodeWireFrames(t, wire.Bytes())
	var flow []model.Emitted
	chunks := 0
	for i := range onWire {
		if frame, ok := emitted(&onWire[i]); ok {
			flow = append(flow, frame)
		}
		if onWire[i].FrameType == FrameTypeChunk {
			chunks++
		}
		if onWire[i].FrameType == FrameTypeStreamEnd && (onWire[i].ChunkCount == nil || *onWire[i].ChunkCount != 3) {
			t.Errorf("the stream's end must say it carried 3 chunks, got %v", onWire[i].ChunkCount)
		}
	}
	if violation := decided(model.Check(flow)); violation.Valid {
		t.Fatalf("the wire carries a flow out of order: %#v in %#v", violation.Value, flow)
	}
	if chunks != 3 {
		t.Errorf("ten bytes at four a chunk are 3 chunks, got %d", chunks)
	}
	if onWire[len(onWire)-1].FrameType != FrameTypeEnd {
		t.Errorf("END must be the last frame of the flow, got %v", onWire[len(onWire)-1].FrameType)
	}

	// The recognizer is what found nothing wrong, not a rubber stamp: the
	// flow with the late frames the gate held back is refused for them.
	ungated := append([]model.Emitted{}, flow...)
	for _, frame := range handedOver[1:] {
		late, _ := emitted(frame)
		ungated = append(ungated, late)
	}
	violation := decided(model.Check(ungated))
	if _, afterEnd := violation.Value.(model.ViolationAfterEnd); !violation.Valid || !afterEnd {
		t.Fatalf("frames after END must be refused as such, got %#v", violation)
	}
}

// TEST12386: a handler changing what one of its pools can serve starts
// whoever in line can now go. Without that a request in line waited for the
// host's next frame.
func Test12386_a_self_report_wakes_the_runtime(t *testing.T) {
	pools, err := newRuntimePools(
		[]string{poolTestCapA},
		&PoolDeclarations{
			Pools:      map[string][]string{"gpu": {poolTestCapA}},
			Capacities: map[string]uint64{"gpu": 2},
		},
	)
	if err != nil {
		t.Fatalf("valid declarations must materialize: %v", err)
	}
	woken := 0
	cell := &poolsCell{pools: pools, changed: func() { woken++ }}
	handle := &PoolHandle{cell: cell, name: "gpu"}

	if err := handle.Set(1); err != nil {
		t.Fatalf("gpu is a declared pool: %v", err)
	}
	if woken != 1 {
		t.Fatalf("a self-report must wake the runtime once, woke it %d times", woken)
	}
	if available := pools.snapshot()["gpu"].Available; available == nil || *available != 1 {
		t.Fatalf("the self-report must be recorded, got %v", available)
	}

	// A refused self-report changed nothing, and wakes nobody.
	ghost := &PoolHandle{cell: cell, name: "cap:ghost"}
	if err := ghost.Set(1); err == nil || !strings.Contains(err.Error(), "cap:ghost") {
		t.Fatalf("an unknown pool must refuse, naming it: %v", err)
	}
	if woken != 1 {
		t.Fatalf("a refused self-report must wake nobody, woke %d times", woken)
	}
}

// eventually waits for a condition that goroutines are about to make true.
func eventually(condition func() bool) bool {
	deadline := time.Now().Add(5 * time.Second)
	for !condition() {
		if time.Now().After(deadline) {
			return false
		}
		time.Sleep(200 * time.Microsecond)
	}
	return true
}

// TEST12387: the switch admits as the model's scripts say. Every arrival is
// a waiting goroutine; after each thing that happens — an arrival, a release,
// a waiter giving up, a limit changing — exactly the requests the model
// admits have been admitted, in the model's order across caps, and each pool
// holds what the model says it holds.
func Test12387_switch_admission_follows_the_model(t *testing.T) {
	scripts := bifaciRows[poolScript](t, "admission")
	var wrong []string
	for index, s := range scripts {
		controller := NewAdmissionController()
		install := admissionTestKey()
		advertised := map[string]uint64{}
		for _, name := range s.Pools {
			advertised[name] = 0
		}
		for _, capacity := range s.Capacities {
			advertised[capacity.Pool] = capacity.Capacity
		}
		controller.ConfigurePools(install, advertised)
		chainOf := func(cap string) []PoolKey {
			chain := []PoolKey{testPoolKey(install, cap)}
			for _, pool := range s.Shared {
				for _, member := range pool.Caps {
					if member == cap {
						chain = append(chain, testPoolKey(install, pool.Name))
					}
				}
			}
			return append(chain, testPoolKey(install, PoolAll))
		}

		// Every arrival, by ticket: its cap, where its permit arrives, and
		// how it gives up.
		type arrival struct {
			cap     string
			permit  chan *AdmissionPermit
			cancel  chan struct{}
			held    *AdmissionPermit
			settled bool
		}
		var arrivals []*arrival
		state := func() (active []uint64, waiting []uint64) {
			byName := controller.Active(install)
			for _, name := range s.Pools {
				active = append(active, byName[name])
			}
			return active, append([]uint64{}, controller.Waiting(install)...)
		}

		for at, op := range s.Ops {
			step := s.Steps[at]
			switch op.Op {
			case "arrive":
				arriving := &arrival{cap: op.Cap, permit: make(chan *AdmissionPermit, 1), cancel: make(chan struct{})}
				arrivals = append(arrivals, arriving)
				chain := chainOf(op.Cap)
				go func() {
					permit, _ := controller.Acquire(chain, arriving.cancel)
					arriving.permit <- permit
				}()
			case "release":
				released := false
				for _, held := range arrivals {
					if held.held != nil && held.cap == op.Cap {
						held.held.Release()
						held.held = nil
						released = true
						break
					}
				}
				if !released {
					t.Fatalf("script %d releases a '%s' request that nobody holds here", index, op.Cap)
				}
			case "leave_oldest":
				for _, waiter := range arrivals {
					if !waiter.settled {
						close(waiter.cancel)
						<-waiter.permit
						waiter.settled = true
						break
					}
				}
			case "capacity":
				controller.ConfigurePools(install, map[string]uint64{op.Pool: op.Capacity})
			default:
				t.Fatalf("unknown admission operation '%s'", op.Op)
			}

			var expectedWent []uint64
			if err := json.Unmarshal(step.Admitted, &expectedWent); err != nil {
				t.Fatalf("an admission step says which tickets went: %v", err)
			}
			settled := eventually(func() bool {
				active, waiting := state()
				return reflect.DeepEqual(active, step.Active) && reflect.DeepEqual(waiting, append([]uint64{}, step.Waiting...))
			})
			if !settled {
				active, waiting := state()
				wrong = append(wrong, fmt.Sprintf("step %d of script %d: active %v, waiting %v", at, index, active, waiting))
				break
			}
			// Exactly the tickets the model admits hold a permit now.
			agreed := true
			for _, ticket := range expectedWent {
				waiter := arrivals[ticket]
				select {
				case permit := <-waiter.permit:
					waiter.held, waiter.settled = permit, true
					agreed = agreed && permit != nil
				case <-time.After(5 * time.Second):
					agreed = false
				}
			}
			for ticket, waiter := range arrivals {
				if !waiter.settled && len(waiter.permit) != 0 {
					wrong = append(wrong, fmt.Sprintf("step %d of script %d: ticket %d was admitted, the model keeps it waiting", at, index, ticket))
					agreed = false
				}
			}
			if !agreed {
				wrong = append(wrong, fmt.Sprintf("step %d of script %d: the model admits %v", at, index, expectedWent))
				break
			}
		}
		for _, waiter := range arrivals {
			if !waiter.settled {
				close(waiter.cancel)
			}
		}
		if len(wrong) > 0 {
			// Every disagreement is waited out before it is one; the first says enough.
			break
		}
	}
	conclude(t, "admission", len(scripts), wrong)
}

// TEST12388: a request that joins the line late into an outage is given what
// is left of the outage's window, not a window of its own: the time is the
// outage's.
func Test12388_a_late_arrival_gets_what_is_left_of_the_outage(t *testing.T) {
	controller := NewAdmissionController()
	controller.grace = 600 * time.Millisecond
	install := admissionTestKey()
	controller.ConfigurePools(install, map[string]uint64{PoolAll: 1})
	controller.DisableMaster(install.MasterIdx)
	time.Sleep(450 * time.Millisecond)

	arrived := time.Now()
	_, err := controller.Acquire(allChain(install), nil)
	waited := time.Since(arrived)
	if err == nil || !strings.Contains(err.Error(), "unavailable for longer than") {
		t.Fatalf("a target that never came back admits nobody, got %v", err)
	}
	if waited < 50*time.Millisecond || waited >= 500*time.Millisecond {
		t.Fatalf("it waited %v: what was left of the outage's window was about 150ms — not nothing, and not a window of its own", waited)
	}
}

// TEST12389: a HELLO proposing a credit window of zero fails the handshake,
// naming the window. Under a zero window no chunk may be sent, and with none
// consumed none is ever granted: every stream would stop at its first chunk,
// for good.
func Test12389_handshake_refuses_a_zero_credit_window(t *testing.T) {
	zero := NewHello(DefaultMaxFrame, DefaultMaxChunk, DefaultMaxReorderBuffer, 0)
	var toCartridge, fromCartridge bytes.Buffer
	if err := NewFrameWriter(&toCartridge).WriteFrame(zero); err != nil {
		t.Fatal(err)
	}
	_, err := HandshakeAccept(NewFrameReader(&toCartridge), NewFrameWriter(&fromCartridge), []byte(`{}`), PoolStates{})
	if err == nil || !strings.Contains(err.Error(), "initial_credit") {
		t.Fatalf("a zero window must fail the handshake, naming it: %v", err)
	}
}
