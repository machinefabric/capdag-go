package bifaci

import (
	"bytes"
	"errors"
	"io"
	"os"
	"sync"
	"testing"
	"time"

	cborlib "github.com/fxamacker/cbor/v2"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	seriesOut    = "media:enc=utf-8;series"
	fitOut       = "media:fmt=json;record;regression"
	residualsOut = "media:fmt=json;list;record;residual"
	noteOut      = "media:enc=utf-8;note"
	rejectedOut  = "media:enc=utf-8;rejected"
)

// regressCap is a cap with four outputs: the main one, a required sequence,
// an optional one, and an optional one that carries the input's type
// unchanged.
func regressCap(t *testing.T) *cap.Cap {
	t.Helper()
	parsed, err := urn.NewCapUrnFromString(`cap:in="` + seriesOut + `";regress;out="` + fitOut + `"`)
	require.NoError(t, err)
	stdin := seriesOut
	c := cap.NewCapWithArgs(parsed, "Regress", []string{"regress"},
		[]cap.CapArg{cap.NewCapArg(seriesOut, "series", true, []cap.ArgSource{{Stdin: &stdin}})})
	// The main output is not the first declared: it is found by its URN.
	residuals := cap.NewCapOutput(residualsOut, "residuals", "Residuals")
	residuals.IsSequence = true
	c.AddOutput(*residuals)
	c.AddOutput(*cap.NewCapOutput(fitOut, "fit", "The fit"))
	c.AddOutput(*cap.NewCapOutput(noteOut, "note", "A note").AsOptional(true))
	c.AddOutput(*cap.NewCapOutput(rejectedOut, "rejected", "Rejected input").AsOptional(true).CarryingInput())
	require.NoError(t, cap.ValidateCapDefinition(c), "the fixture is a valid definition")
	return c
}

// recordedOutputs is the outputs of `capUrn` on streams that write to memory.
type recordedOutputs struct {
	outputs *responseOutputs
	writer  *syncFrameWriter
	wire    *bytes.Buffer
}

func newRecordedOutputs(t *testing.T, capUrn string, manifest *CapManifest) *recordedOutputs {
	t.Helper()
	recorded := &recordedOutputs{wire: &bytes.Buffer{}}
	recorded.writer = newSyncFrameWriter(NewFrameWriter(recorded.wire), NewDropCounters(), NewStragglerCounters())
	rid := NewMessageIdRandom()
	opened := 0
	outputs, err := responseOutputsForCap(capUrn, manifest, func(label string, output *string) outputStream {
		opened++
		stream := newThreadSafeEmitter(recorded.writer, rid, nil, "s"+string(rune('0'+opened)), label, DefaultMaxChunk, nil, 0)
		stream.output = output
		return stream
	})
	require.NoError(t, err, "the cap's outputs")
	for _, output := range outputs.all() {
		output.stream.(*threadSafeEmitter).outputs = outputs
	}
	recorded.outputs = outputs
	return recorded
}

// started is one STREAM_START that was sent.
type started struct {
	output     string // the output it names; "" for none
	label      string
	isSequence bool
	streamID   string
}

func (r *recordedOutputs) frames(t *testing.T) []Frame {
	t.Helper()
	return decodeWireFrames(t, r.wire.Bytes())
}

func startsOf(frames []Frame) []started {
	var sent []started
	for _, frame := range frames {
		if frame.FrameType != FrameTypeStreamStart {
			continue
		}
		each := started{label: *frame.MediaUrn, streamID: *frame.StreamId}
		if frame.Output != nil {
			each.output = *frame.Output
		}
		if frame.IsSequence != nil {
			each.isSequence = *frame.IsSequence
		}
		sent = append(sent, each)
	}
	return sent
}

func startNaming(t *testing.T, sent []started, output string) started {
	t.Helper()
	for _, each := range sent {
		if each.output == output {
			return each
		}
	}
	t.Fatalf("no STREAM_START names %q: %+v", output, sent)
	return started{}
}

// TEST12719: a request answers on every output its cap declares. Each has a
// stream of its own, carrying the label of what it holds: the main output the
// label the cap's effect derives, another output its own URN, and one that
// carries the input the cap's declared input. The main output's stream names
// no output — it is the cap's answer — and every other stream names the
// output it is. An output is found by its media URN as a tagged URN; one the
// cap does not declare is an UnknownOutputError.
func Test12719_a_request_answers_on_every_declared_output(t *testing.T) {
	definition := regressCap(t)
	manifest := createTestManifest("Regress", "1.0.0", "Test", []*cap.Cap{definition})
	recorded := newRecordedOutputs(t, definition.UrnString(), manifest)
	req := &Request{emitter: recorded.outputs.main.stream}

	// The main output is the one whose URN is `out=`, wherever it is declared.
	stream := func(mediaUrn string) StreamEmitter {
		found, err := req.OutputFor(mediaUrn)
		require.NoError(t, err, mediaUrn)
		return found
	}
	assert.Same(t, req.Output(), stream(fitOut))
	assert.Same(t, req.Output(), stream("media:regression;record;fmt=json"))
	// Another output, with its tags in another order, is the same stream.
	residuals := stream(residualsOut)
	assert.Same(t, residuals, stream("media:residual;record;list;fmt=json"))
	assert.NotSame(t, residuals, req.Output())
	// Any stream of the request leads to the others.
	fromResiduals, err := OutputFor(residuals, noteOut)
	require.NoError(t, err)
	assert.Same(t, stream(noteOut), fromResiduals)

	for _, each := range []struct {
		urn      string
		sequence bool
	}{{fitOut, false}, {residualsOut, true}, {noteOut, false}, {rejectedOut, false}} {
		output := stream(each.urn).(outputStream)
		assert.False(t, output.isStarted(), each.urn)
		require.NoError(t, output.startDeclared(each.sequence))
		assert.True(t, output.isStarted(), each.urn)
		require.NoError(t, output.closeStream())
	}
	sent := startsOf(recorded.frames(t))
	require.Len(t, sent, 4)
	// Exactly one stream names no output: the main one.
	var unnamed []started
	for _, each := range sent {
		if each.output == "" {
			unnamed = append(unnamed, each)
		}
		assert.NotEqual(t, fitOut, each.output, "the main output is not named")
	}
	require.Len(t, unnamed, 1, "%+v", sent)
	assert.Equal(t, fitOut, unnamed[0].label, "the main output carries the label the effect derives")
	assert.Equal(t, residualsOut, startNaming(t, sent, residualsOut).label, "another output carries its own URN")
	assert.True(t, startNaming(t, sent, residualsOut).isSequence)
	assert.Equal(t, noteOut, startNaming(t, sent, noteOut).label)
	assert.Equal(t, seriesOut, startNaming(t, sent, rejectedOut).label, "an output that carries the input is labelled with it")
	ids := map[string]bool{}
	for _, each := range sent {
		ids[each.streamID] = true
	}
	assert.Len(t, ids, 4, "each output has a stream of its own")

	// What the cap does not declare is not an output of it — a tag short or a
	// tag more is another URN.
	for _, unknown := range []string{"media:enc=utf-8", "media:fmt=json;record", "media:enc=utf-8;note;extra", "not a urn"} {
		_, err := req.OutputFor(unknown)
		var refused *UnknownOutputError
		require.ErrorAs(t, err, &refused, unknown)
		assert.Equal(t, definition.UrnString(), refused.CapUrn)
		assert.Equal(t, unknown, refused.Output)
		assert.Contains(t, err.Error(), "declares no output '"+unknown+"'")
	}

	// A stream that belongs to no declared set of outputs is the one output
	// there is: nothing is known of it but that it is the answer.
	bare := newThreadSafeEmitter(recorded.writer, NewMessageIdRandom(), nil, "bare", "media:", DefaultMaxChunk, nil, 0)
	only, err := OutputFor(bare, "media:")
	require.NoError(t, err)
	assert.Same(t, StreamEmitter(bare), only)
	_, err = OutputFor(bare, noteOut)
	var refused *UnknownOutputError
	require.ErrorAs(t, err, &refused)
	_, err = OutputFor(bare, "not a urn")
	require.ErrorAs(t, err, &refused)
}

// liveRun runs `handler` as the handler of one request for `capUrn`, the way
// the runtime does, and returns every frame it put on the wire.
func liveRun(t *testing.T, capUrn string, manifest *CapManifest, handler HandlerFunc) []Frame {
	t.Helper()
	var wire bytes.Buffer
	writer := newSyncFrameWriter(NewFrameWriter(&wire), NewDropCounters(), NewStragglerCounters())
	input := newUnboundedFrameChan()
	input.Close()
	runLiveHandler(&liveHandlerRequest{
		requestID: NewMessageIdRandom(),
		capUrn:    capUrn,
		handler:   handler,
		frames:    input,
		terminal:  &terminalClaim{},
	}, manifest, writer, DefaultLimits(), &sync.Map{}, nil)
	return decodeWireFrames(t, wire.Bytes())
}

func countOf(frames []Frame, frameType FrameType) int {
	count := 0
	for _, frame := range frames {
		if frame.FrameType == frameType {
			count++
		}
	}
	return count
}

// residualsOnly writes two residual rows and nothing else, or fails after.
func residualsOnly(fail bool) HandlerFunc {
	return func(_ <-chan Frame, emitter StreamEmitter, _ PeerInvoker) error {
		residuals, err := OutputFor(emitter, residualsOut)
		if err != nil {
			return err
		}
		for _, row := range []int{1, 2} {
			if err := residuals.EmitListItem(row); err != nil {
				return err
			}
		}
		if fail {
			return errors.New("no fit")
		}
		return nil
	}
}

func silent(<-chan Frame, StreamEmitter, PeerInvoker) error { return nil }

// TEST12720: what the runtime sends for outputs the handler did not write to,
// once it has returned. A required output is sent empty in its declared
// cardinality, so its consumers get a value; an optional one is not sent — it
// did not fire. A handler that failed gets nothing sent on its behalf.
func Test12720_unwritten_outputs_when_the_handler_returns(t *testing.T) {
	definition := regressCap(t)
	manifest := createTestManifest("Regress", "1.0.0", "Test", []*cap.Cap{definition})

	all := liveRun(t, definition.UrnString(), manifest, residualsOnly(false))
	sent := startsOf(all)
	require.Len(t, sent, 2, "the written output and the required unwritten one: %+v", sent)
	named := []string{}
	for _, each := range sent {
		if each.output != "" {
			named = append(named, each.output)
		}
	}
	assert.Equal(t, []string{residualsOut}, named, "the residuals are named; optional outputs did not fire")
	// The other stream is the main output's: it names none.
	fit := startNaming(t, sent, "")
	assert.Equal(t, fitOut, fit.label)
	assert.False(t, fit.isSequence, "the fit is declared a single value")
	// The empty stream is a whole stream: STREAM_END promising no chunks.
	var fitFrames []Frame
	for _, frame := range all {
		if frame.StreamId != nil && *frame.StreamId == fit.streamID {
			fitFrames = append(fitFrames, frame)
		}
	}
	require.Len(t, fitFrames, 2, "STREAM_START and STREAM_END only")
	require.Equal(t, FrameTypeStreamEnd, fitFrames[1].FrameType)
	require.NotNil(t, fitFrames[1].ChunkCount)
	assert.Equal(t, uint64(0), *fitFrames[1].ChunkCount)
	// Every stream that was opened was closed, and then the request.
	assert.Equal(t, 2, countOf(all, FrameTypeStreamEnd))
	assert.Equal(t, 1, countOf(all, FrameTypeEnd))
	assert.Equal(t, FrameTypeEnd, all[len(all)-1].FrameType, "END comes after every stream's end")
	assert.Equal(t, 0, countOf(all, FrameTypeErr))

	// The stream a handler is given is its cap's main output: what it writes
	// there is the cap's answer, on the stream that names no output.
	all = liveRun(t, definition.UrnString(), manifest, func(_ <-chan Frame, emitter StreamEmitter, _ PeerInvoker) error {
		return emitter.EmitCbor([]byte("the fit"))
	})
	sent = startsOf(all)
	answer := startNaming(t, sent, "")
	wrote := 0
	for _, frame := range all {
		if frame.FrameType == FrameTypeChunk {
			wrote++
			assert.Equal(t, answer.streamID, *frame.StreamId, "the chunk is on the main output's stream")
		}
	}
	assert.Equal(t, 1, wrote)

	// A required SEQUENCE output nobody wrote to is an empty sequence.
	all = liveRun(t, definition.UrnString(), manifest, silent)
	sent = startsOf(all)
	require.Len(t, sent, 2, "%+v", sent)
	assert.True(t, startNaming(t, sent, residualsOut).isSequence, "the residuals are declared a sequence")
	assert.Equal(t, 2, countOf(all, FrameTypeStreamEnd))

	// A failed handler: only what it wrote itself, and that left open — the
	// request ends with ERR, not with outputs made up for it.
	all = liveRun(t, definition.UrnString(), manifest, residualsOnly(true))
	sent = startsOf(all)
	require.Len(t, sent, 1, "%+v", sent)
	assert.Equal(t, residualsOut, sent[0].output)
	assert.Equal(t, 0, countOf(all, FrameTypeStreamEnd))
	assert.Equal(t, 0, countOf(all, FrameTypeEnd))
	assert.Equal(t, 1, countOf(all, FrameTypeErr))

	// A cap whose `out=` is void has no output: nothing is opened for it, and
	// the request still ends.
	all = liveRun(t, `cap:in="media:enc=utf-8";sink;out=media:void`, nil, silent)
	assert.Empty(t, startsOf(all))
	require.Len(t, all, 1)
	assert.Equal(t, FrameTypeEnd, all[0].FrameType)

	// The main output's label follows the cap's effect, and is not its name:
	// a cap that passes its input through labels the stream with the input's
	// type and names the output its `out=`.
	pass := `cap:effect=none;in="media:enc=utf-8;line";out="media:enc=utf-8";pass`
	sent = startsOf(liveRun(t, pass, nil, silent))
	require.Len(t, sent, 1)
	assert.Equal(t, "", sent[0].output, "the main output's stream names no output")
	derived, err := DeriveResponseMedia(pass)
	require.NoError(t, err)
	assert.Equal(t, derived, sent[0].label)
	assert.NotEqual(t, "media:enc=utf-8", sent[0].label, "the label is the input's type")

	// A cap served without a definition answers on the one output its URN states.
	sent = startsOf(liveRun(t, `cap:in="media:enc=utf-8";count;out="media:integer"`, nil, silent))
	require.Len(t, sent, 1)
	assert.Equal(t, started{label: "media:integer", streamID: sent[0].streamID}, sent[0])

	// An output the definition marks optional and the handler does not write
	// is not sent even when it is the main one.
	parsed, err := urn.NewCapUrnFromString(`cap:in="media:enc=utf-8";maybe;out="media:enc=utf-8;found"`)
	require.NoError(t, err)
	stdin := "media:enc=utf-8"
	maybe := cap.NewCapWithArgs(parsed, "Maybe", []string{"maybe"},
		[]cap.CapArg{cap.NewCapArg("media:enc=utf-8", "text", true, []cap.ArgSource{{Stdin: &stdin}})})
	maybe.AddOutput(*cap.NewCapOutput("media:enc=utf-8;found", "found", "What was found").AsOptional(true))
	require.NoError(t, cap.ValidateCapDefinition(maybe))
	all = liveRun(t, maybe.UrnString(), createTestManifest("Maybe", "1.0.0", "Test", []*cap.Cap{maybe}), silent)
	assert.Empty(t, startsOf(all))
	assert.Equal(t, 1, countOf(all, FrameTypeEnd))

	// A MAIN output declared a sequence and never written is an empty
	// sequence too: the stream says what the definition says.
	parsed, err = urn.NewCapUrnFromString(`cap:in="media:enc=utf-8";lines;out="media:enc=utf-8;line"`)
	require.NoError(t, err)
	lines := cap.NewCapWithArgs(parsed, "Lines", []string{"lines"},
		[]cap.CapArg{cap.NewCapArg("media:enc=utf-8", "text", true, []cap.ArgSource{{Stdin: &stdin}})})
	every := cap.NewCapOutput("media:enc=utf-8;line", "lines", "Every line")
	every.IsSequence = true
	lines.AddOutput(*every)
	require.NoError(t, cap.ValidateCapDefinition(lines))
	sent = startsOf(liveRun(t, lines.UrnString(), createTestManifest("Lines", "1.0.0", "Test", []*cap.Cap{lines}), silent))
	require.Len(t, sent, 1)
	assert.Equal(t, "", sent[0].output)
	assert.True(t, sent[0].isSequence, "the main output is declared a sequence")
}

// TEST12722: a command line has one standard output, and it carries the cap's
// main output. A stream of another output is recognised by the output it is —
// not by what it carries, which may be the same type — and its data is not
// printed.
func Test12722_standard_output_carries_the_main_output(t *testing.T) {
	definition := regressCap(t)
	manifest := createTestManifest("Regress", "1.0.0", "Test", []*cap.Cap{definition})
	outputs, err := responseOutputsForCap(definition.UrnString(), manifest, func(_ string, output *string) outputStream {
		return &cliStreamEmitter{unprinted: output != nil}
	})
	require.NoError(t, err)
	for _, output := range outputs.all() {
		output.stream.(*cliStreamEmitter).outputs = outputs
	}
	main := outputs.main.stream.(*cliStreamEmitter)
	assert.False(t, main.unprinted, "the main output is printed")
	require.Len(t, outputs.others, 3)
	for _, mediaUrn := range []string{residualsOut, noteOut, rejectedOut} {
		other, err := OutputFor(main, mediaUrn)
		require.NoError(t, err, mediaUrn)
		stream := other.(*cliStreamEmitter)
		assert.True(t, stream.unprinted, mediaUrn)
		// What a handler writes to it is accepted without being read: a value
		// the printed stream refuses is taken here.
		require.NoError(t, stream.EmitCbor(42), "an unprinted stream takes any value")
		require.NoError(t, stream.EmitListItem(map[string]interface{}{"no": "value field"}))
	}
	// The same value on the printed stream is read, and refused.
	assert.Error(t, main.EmitCbor(42))
	// Nothing is owed on a command line for an output nobody wrote to.
	require.NoError(t, outputs.finish())

	// Through the command-line dispatch itself: what the handler writes to
	// the stream it is given reaches standard output, and what it writes to
	// another output does not.
	runtime, err := NewCartridgeRuntimeWithManifest(manifest)
	require.NoError(t, err)
	handler := func(input <-chan Frame, emitter StreamEmitter, _ PeerInvoker) error {
		for range input {
		}
		if err := emitter.Write([]byte("the fit")); err != nil {
			return err
		}
		note, err := OutputFor(emitter, noteOut)
		if err != nil {
			return err
		}
		if err := note.Write([]byte("a note")); err != nil {
			return err
		}
		return note.EmitCbor([]byte("another note"))
	}
	reader, writer, err := os.Pipe()
	require.NoError(t, err)
	saved := os.Stdout
	os.Stdout = writer
	dispatchErr := runtime.dispatchCliPayload(definition, handler, nil)
	os.Stdout = saved
	require.NoError(t, writer.Close())
	printed, err := io.ReadAll(reader)
	require.NoError(t, err)
	require.NoError(t, dispatchErr)
	assert.Equal(t, "the fit", string(printed))
}

// valueChunk is one CHUNK of `stream` carrying `value`.
func valueChunk(t *testing.T, rid MessageId, stream string, index uint64, value interface{}) Frame {
	t.Helper()
	payload, err := cborlib.Marshal(value)
	require.NoError(t, err)
	return *NewChunk(rid, stream, 0, payload, index, ComputeChecksum(payload))
}

func feedFrames(frames ...Frame) <-chan Frame {
	ch := make(chan Frame, len(frames))
	for _, frame := range frames {
		ch <- frame
	}
	close(ch)
	return ch
}

// TEST12730: a peer call's response has one stream per output the callee
// answers on. The stream that names no output is the callee's main output:
// its items are the response's data. A stream that names an output yields
// items tagged with it, and they are not part of the data a caller collects.
// The response ends at the request's END, not at the first stream's end.
func Test12730_a_peer_response_has_a_stream_per_output(t *testing.T) {
	rid := NewMessageIdRandom()
	const rest = "media:enc=utf-8;split-rest"
	single := false
	frames := func() <-chan Frame {
		return feedFrames(
			// The other output starts first, ends first, and carries the
			// same type as the main one.
			*NewStreamStart(rid, "rest", "media:enc=utf-8", &single).ForOutput(rest),
			valueChunk(t, rid, "rest", 0, []byte("R1")),
			*NewStreamStart(rid, "head", "media:enc=utf-8", &single),
			valueChunk(t, rid, "head", 0, []byte("H1")),
			valueChunk(t, rid, "rest", 1, []byte("R2")),
			*NewStreamEnd(rid, "rest", 2),
			valueChunk(t, rid, "head", 1, []byte("H2")),
			*NewStreamEnd(rid, "head", 2),
			*NewEnd(rid, nil),
		)
	}

	// Item by item: which output each belongs to, in arrival order.
	type seen struct{ output, text string }
	var items []seen
	response := DemuxPeerResponse(frames())
	for {
		item, more := response.Recv()
		if !more {
			break
		}
		require.NoError(t, item.DataErr)
		require.Nil(t, item.LogFrame)
		output := item.Output
		if item.IsDataItem {
			require.Equal(t, "", item.Output, "data of the main output names none")
			output = "main"
		}
		items = append(items, seen{output, string(item.DataValue.([]byte))})
	}
	assert.Equal(t, []seen{{rest, "R1"}, {"main", "H1"}, {rest, "R2"}, {"main", "H2"}}, items,
		"everything up to END, each item under its own output")

	// A caller that collects the cap's answer gets the main output only.
	collected, err := DemuxPeerResponse(frames()).CollectBytes()
	require.NoError(t, err)
	assert.Equal(t, "H1H2", string(collected))

	// One value is the main output's one value, whatever else was answered.
	value, err := DemuxPeerResponse(feedFrames(
		*NewStreamStart(rid, "rest", "media:enc=utf-8", &single).ForOutput(rest),
		valueChunk(t, rid, "rest", 0, []byte("R1")),
		*NewStreamEnd(rid, "rest", 1),
		*NewStreamStart(rid, "head", "media:enc=utf-8", &single),
		valueChunk(t, rid, "head", 0, []byte("H1")),
		*NewStreamEnd(rid, "head", 1),
		*NewEnd(rid, nil),
	)).CollectValue()
	require.NoError(t, err)
	assert.Equal(t, []byte("H1"), value)

	// A chunk for a stream that was never started is not data of any output.
	stray := DemuxPeerResponse(feedFrames(valueChunk(t, rid, "ghost", 0, []byte("?")), *NewEnd(rid, nil)))
	item, more := stray.Recv()
	require.True(t, more)
	require.Error(t, item.DataErr)
	assert.Contains(t, item.DataErr.Error(), "was not started: ghost")
	_, more = stray.Recv()
	assert.False(t, more)

	// A response whose frames stop before END was cut short, however many of
	// its streams had ended.
	_, err = DemuxPeerResponse(feedFrames(
		*NewStreamStart(rid, "head", "media:enc=utf-8", &single),
		valueChunk(t, rid, "head", 0, []byte("H1")),
		*NewStreamEnd(rid, "head", 1),
	)).CollectBytes()
	assert.ErrorIs(t, err, ErrAbandoned)

	// A sequence stream of another output that stops mid-item is an error of
	// that output, at its end — and at END for a stream that never ended.
	sequence := true
	whole, err := cborlib.Marshal("a long enough item")
	require.NoError(t, err)
	half := whole[:len(whole)/2]
	for _, ending := range [][]Frame{
		{*NewStreamEnd(rid, "rows", 1), *NewEnd(rid, nil)},
		{*NewEnd(rid, nil)},
	} {
		sent := []Frame{
			*NewStreamStart(rid, "rows", "media:enc=utf-8", &sequence).ForOutput(rest),
			*NewChunk(rid, "rows", 0, half, 0, ComputeChecksum(half)),
		}
		partial := DemuxPeerResponse(feedFrames(append(sent, ending...)...))
		item, more := partial.Recv()
		require.True(t, more)
		assert.Equal(t, rest, item.Output)
		assert.False(t, item.IsDataItem)
		assert.Error(t, item.DataErr, "an item cut short")
		_, more = partial.Recv()
		assert.False(t, more)
	}

	// Over the wire: as a cartridge consumes a peer's response it credits
	// each stream by name. A grant that names no stream reaches a callee
	// with several only by guessing.
	runtime, err := NewCartridgeRuntime([]byte(testManifest))
	require.NoError(t, err)
	answer := make(chan []byte, 1)
	runtime.Register(testCapUrn, func(input <-chan Frame, _ StreamEmitter, peer PeerInvoker) error {
		for range input {
		}
		reply, err := peer.Invoke(`cap:in="media:void";split;out="media:enc=utf-8"`, nil)
		if err != nil {
			return err
		}
		data, err := DemuxPeerResponse(reply).CollectBytes()
		if err != nil {
			return err
		}
		answer <- data
		return nil
	})
	// A window of four: a grant goes out for every two chunks consumed.
	hostWriter, sent, _ := startTestCartridge(t, runtime, 4)
	caller := NewMessageIdRandom()
	require.NoError(t, hostWriter.WriteFrame(NewReq(caller, testCapUrn, []byte{}, "application/cbor")))
	require.NoError(t, hostWriter.WriteFrame(NewEnd(caller, nil)))
	next := func() Frame {
		select {
		case frame, open := <-sent:
			require.True(t, open, "the runtime stopped")
			return frame
		case <-time.After(20 * time.Second):
			t.Fatal("the runtime sent nothing")
			return Frame{}
		}
	}
	// The cartridge calls its peer: its REQ, then the END of its arguments.
	var callee MessageId
	for called := false; !called; {
		frame := next()
		switch {
		case frame.FrameType == FrameTypeReq:
			callee = frame.Id
		case frame.FrameType == FrameTypeEnd && frame.Id.ToString() == callee.ToString():
			called = true
		}
	}
	// The peer answers on two streams, four chunks each.
	require.NoError(t, hostWriter.WriteFrame(NewStreamStart(callee, "rest", "media:enc=utf-8", &single).ForOutput(rest)))
	require.NoError(t, hostWriter.WriteFrame(NewStreamStart(callee, "head", "media:enc=utf-8", &single)))
	for index := uint64(0); index < 4; index++ {
		for _, stream := range []string{"rest", "head"} {
			chunk := valueChunk(t, callee, stream, index, []byte{stream[0], byte('0' + index)})
			require.NoError(t, hostWriter.WriteFrame(&chunk))
		}
	}
	require.NoError(t, hostWriter.WriteFrame(NewStreamEnd(callee, "rest", 4)))
	require.NoError(t, hostWriter.WriteFrame(NewStreamEnd(callee, "head", 4)))
	require.NoError(t, hostWriter.WriteFrame(NewEnd(callee, nil)))

	granted := map[string]uint64{}
	for ended := false; !ended; {
		frame := next()
		switch {
		case frame.FrameType == FrameTypeCredit && frame.Id.ToString() == callee.ToString():
			require.NotNil(t, frame.StreamId, "every grant names its stream")
			require.NotNil(t, frame.Credit)
			granted[*frame.StreamId] += *frame.Credit
		case frame.FrameType == FrameTypeErr:
			t.Fatalf("the request failed: %s", frame.ErrorMessage())
		case frame.FrameType == FrameTypeEnd && frame.Id.ToString() == caller.ToString():
			ended = true
		}
	}
	assert.Equal(t, "h0h1h2h3", string(<-answer), "the caller collected the main output")
	require.Len(t, granted, 2, "%v", granted)
	// Each stream's grants cover what was consumed of THAT stream — half a
	// window at a time, four chunks each — although the two streams' chunks
	// arrived in turn: a stream is not credited for what another delivered.
	assert.Equal(t, uint64(4), granted["rest"], "%v", granted)
	assert.Equal(t, uint64(4), granted["head"], "%v", granted)
}

// driveOneRequest sends one request's frames to `runtime` over the wire and
// returns the frames of that request it sent back, up to the terminal one.
func driveOneRequest(t *testing.T, runtime *CartridgeRuntime, rid MessageId, frames ...*Frame) []Frame {
	t.Helper()
	hostWriter, sent, _ := startTestCartridge(t, runtime, DefaultInitialCredit)
	for _, frame := range frames {
		require.NoError(t, hostWriter.WriteFrame(frame))
	}
	var reply []Frame
	deadline := time.After(20 * time.Second)
	for {
		select {
		case frame, open := <-sent:
			if !open {
				t.Fatalf("the runtime stopped before ending the request: %v", reply)
			}
			if frame.Id.ToString() != rid.ToString() {
				continue
			}
			reply = append(reply, frame)
			if frame.FrameType == FrameTypeEnd || frame.FrameType == FrameTypeErr {
				return reply
			}
		case <-deadline:
			t.Fatalf("the request was not ended: %v", reply)
		}
	}
}

// TEST12731: a request stream names no output: an argument is identified by
// its media URN alone. A request stream that does name one is refused where
// it is read — the request ends with a protocol error, nothing of the stream
// reaches the handler as an argument, and the handler is not left waiting:
// its input ends short of END.
func Test12731_a_request_stream_that_names_an_output_is_refused(t *testing.T) {
	const capUrn = `cap:in="media:enc=utf-8";echo-named;out="media:enc=utf-8"`
	run := func(named bool) (reply []Frame, handled []Frame) {
		runtime, err := NewCartridgeRuntimeWithManifest(createTestManifest("Echo", "1.0.0", "Test", nil))
		require.NoError(t, err)
		returned := make(chan []Frame, 1)
		runtime.Register(capUrn, func(frames <-chan Frame, emitter StreamEmitter, _ PeerInvoker) error {
			var read []Frame
			for frame := range frames {
				read = append(read, frame)
			}
			returned <- read
			if len(read) == 0 || read[len(read)-1].FrameType != FrameTypeEnd {
				return ErrAbandoned
			}
			return emitter.EmitCbor([]byte("ok"))
		})
		rid := NewMessageIdRandom()
		start := NewStreamStart(rid, "arg0", "media:enc=utf-8", nil)
		if named {
			start.ForOutput("media:enc=utf-8;kept")
		}
		payload, err := cborlib.Marshal([]byte("x"))
		require.NoError(t, err)
		reply = driveOneRequest(t, runtime, rid,
			NewReq(rid, capUrn, []byte{}, "application/cbor"),
			start,
			NewChunk(rid, "arg0", 0, payload, 0, ComputeChecksum(payload)),
			NewStreamEnd(rid, "arg0", 1),
			NewEnd(rid, nil),
		)
		select {
		case handled = <-returned:
		case <-time.After(20 * time.Second):
			t.Fatal("the handler was left waiting for input")
		}
		return reply, handled
	}

	// An argument, as every request stream is.
	reply, handled := run(false)
	assert.Equal(t, 0, countOf(reply, FrameTypeErr))
	assert.Equal(t, 1, countOf(reply, FrameTypeEnd))
	assert.Equal(t, 1, countOf(handled, FrameTypeStreamStart), "the argument reaches the handler")
	assert.Equal(t, 1, countOf(handled, FrameTypeEnd))

	// The same stream naming an output: a response stream where an argument
	// belongs.
	reply, handled = run(true)
	refused := reply[len(reply)-1]
	require.Equal(t, FrameTypeErr, refused.FrameType, "the request is refused: %v", reply)
	assert.Equal(t, "PROTOCOL_ERROR", refused.ErrorCode())
	assert.Contains(t, refused.ErrorMessage(), "names an output")
	assert.Contains(t, refused.ErrorMessage(), "media:enc=utf-8;kept")
	assert.Equal(t, 0, countOf(reply, FrameTypeEnd))
	assert.Equal(t, 0, countOf(handled, FrameTypeStreamStart), "nothing of the request is read as an argument")
	assert.Equal(t, 0, countOf(handled, FrameTypeChunk))
	assert.Equal(t, 0, countOf(handled, FrameTypeEnd), "the handler's input ends short of END")
}
