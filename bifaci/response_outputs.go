package bifaci

import (
	"fmt"

	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// The outputs a request answers on (13.2 §Outputs).
//
// A cap answers with one stream per output its definition declares. The
// stream of its main output names none — it is the cap's answer; the stream of
// another output names the output it is (STREAM_START key 21).

// UnknownOutputError says a handler asked for an output its cap does not
// declare.
type UnknownOutputError struct {
	CapUrn string
	Output string
}

func (e *UnknownOutputError) Error() string {
	return fmt.Sprintf("Cap '%s' declares no output '%s'", e.CapUrn, e.Output)
}

// outputStream is a response stream the runtime can open and close on a
// handler's behalf.
type outputStream interface {
	StreamEmitter
	// isStarted reports whether the stream's STREAM_START was sent.
	isStarted() bool
	// startDeclared opens the stream, empty, in the cardinality its output
	// declares.
	startDeclared(isSequence bool) error
	// closeStream ends a started stream.
	closeStream() error
}

// responseOutput is one output a request answers on, and its stream.
type responseOutput struct {
	// urn is the output's media URN: its identity, and — for an output other
	// than the main one — what its STREAM_START names.
	urn *urn.MediaUrn
	// required: an output the handler never wrote to is sent as an empty
	// value when the handler returns; an optional one is not sent — it did
	// not fire.
	required bool
	// isSequence is the declared cardinality, which an empty stream opened
	// by the runtime states in the handler's place.
	isSequence bool
	stream     outputStream
}

// responseOutputs is the outputs of the cap a request is for: its main output
// and every other output its definition declares, each with a stream of its
// own.
type responseOutputs struct {
	capUrn string
	main   responseOutput
	others []responseOutput
}

// responseOutputsForCap returns the outputs of `capUrn` as `manifest` defines
// it.
//
// `open` builds one stream from the media label its STREAM_START carries and
// the output it names (nil for the main output). The main output carries the
// label the cap URN's effect derives (DeriveResponseMedia) and names no
// output; another output carries its own media URN, or the cap's declared
// input when it carries the input, and names itself. A cap served without a
// definition in the manifest (a built-in the cartridge did not advertise)
// answers on the one output its URN states.
func responseOutputsForCap(
	capUrn string,
	manifest *CapManifest,
	open func(label string, output *string) outputStream,
) (*responseOutputs, error) {
	parsed, err := urn.NewCapUrnFromString(capUrn)
	if err != nil {
		return nil, fmt.Errorf("'%s': %w", capUrn, err)
	}
	parse := func(text string) (*urn.MediaUrn, error) {
		media, err := urn.NewMediaUrnFromString(text)
		if err != nil {
			return nil, fmt.Errorf("cap '%s' declares an output that is not a media URN ('%s'): %w", capUrn, text, err)
		}
		return media, nil
	}
	var definition *cap.Cap
	if manifest != nil {
		advertised := manifest.AllCaps()
		for i := range advertised {
			if advertised[i].Urn.Equals(parsed) {
				definition = &advertised[i]
				break
			}
		}
	}
	var declaredMain *cap.CapOutput
	if definition != nil {
		declaredMain = definition.MainOutput()
	}

	outSpec, err := parse(parsed.OutSpec())
	if err != nil {
		return nil, err
	}
	void, err := parse(standard.MediaVoid)
	if err != nil {
		return nil, err
	}
	mainLabel, err := DeriveResponseMedia(capUrn)
	if err != nil {
		return nil, err
	}

	outputs := &responseOutputs{capUrn: capUrn}
	outputs.main = responseOutput{
		urn: outSpec,
		// A cap whose `out=` is void has no output: its stream exists for
		// progress and logs, and is never opened on the handler's behalf.
		required: !outSpec.IsEquivalent(void),
		stream:   open(mainLabel, nil),
	}
	if declaredMain != nil {
		if outputs.main.urn, err = parse(declaredMain.MediaUrn); err != nil {
			return nil, err
		}
		outputs.main.required = outputs.main.required && !declaredMain.Optional
		outputs.main.isSequence = declaredMain.IsSequence
	}

	if definition == nil {
		return outputs, nil
	}
	for i := range definition.Outputs {
		output := &definition.Outputs[i]
		if output == declaredMain {
			continue
		}
		outputUrn, err := parse(output.MediaUrn)
		if err != nil {
			return nil, err
		}
		label := output.MediaUrn
		if output.Carries != nil {
			label = parsed.InSpec()
		}
		named := output.MediaUrn
		outputs.others = append(outputs.others, responseOutput{
			urn:        outputUrn,
			required:   !output.Optional,
			isSequence: output.IsSequence,
			stream:     open(label, &named),
		})
	}
	return outputs, nil
}

func (o *responseOutputs) all() []*responseOutput {
	every := []*responseOutput{&o.main}
	for i := range o.others {
		every = append(every, &o.others[i])
	}
	return every
}

// streamFor returns the stream of the output `mediaUrn` identifies, by
// tagged-URN equivalence — the way an input stream is found.
func (o *responseOutputs) streamFor(mediaUrn string) (StreamEmitter, error) {
	wanted, err := urn.NewMediaUrnFromString(mediaUrn)
	if err != nil {
		return nil, &UnknownOutputError{CapUrn: o.capUrn, Output: mediaUrn}
	}
	for _, output := range o.all() {
		if output.urn.IsEquivalent(wanted) {
			return output.stream, nil
		}
	}
	return nil, &UnknownOutputError{CapUrn: o.capUrn, Output: mediaUrn}
}

// finish ends every output once the handler has returned successfully: a
// stream the handler wrote to is closed; a required output it never wrote to
// is opened and closed empty, in its declared cardinality; an optional one it
// never wrote to sends nothing.
func (o *responseOutputs) finish() error {
	for _, output := range o.all() {
		if !output.stream.isStarted() {
			if !output.required {
				continue
			}
			if err := output.stream.startDeclared(output.isSequence); err != nil {
				return err
			}
		}
		if err := output.stream.closeStream(); err != nil {
			return err
		}
	}
	return nil
}

// outputSet is implemented by the emitters the runtime gives a handler: each
// knows the outputs of the request it belongs to.
type outputSet interface {
	requestOutputs() *responseOutputs
}

// OutputFor returns the stream of the declared output `mediaUrn` identifies
// (tagged-URN equivalence), given any stream of the same request — the one a
// handler is handed is its cap's main output. The main output's URN gives the
// main output's stream. A URN the cap does not declare is an
// *UnknownOutputError.
//
// A stream that belongs to no declared set of outputs (one a caller built
// itself) is the one output there is, and nothing is known of it.
func OutputFor(emitter StreamEmitter, mediaUrn string) (StreamEmitter, error) {
	if set, declared := emitter.(outputSet); declared {
		if outputs := set.requestOutputs(); outputs != nil {
			return outputs.streamFor(mediaUrn)
		}
	}
	wanted, err := urn.NewMediaUrnFromString(mediaUrn)
	if err != nil {
		return nil, &UnknownOutputError{Output: mediaUrn}
	}
	identity, err := urn.NewMediaUrnFromString(standard.MediaIdentity)
	if err != nil {
		panic(fmt.Sprintf("the identity media URN does not parse: %v", err))
	}
	if !identity.IsEquivalent(wanted) {
		return nil, &UnknownOutputError{Output: mediaUrn}
	}
	return emitter, nil
}

// OutputFor returns the stream of the declared output `mediaUrn` identifies.
// The main output's URN gives the same stream as Output.
func (r *Request) OutputFor(mediaUrn string) (StreamEmitter, error) {
	return OutputFor(r.emitter, mediaUrn)
}
