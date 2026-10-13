package machine

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"

	"github.com/machinefabric/capdag-go/urn"
)

// Machines in step position: where another machine's notation comes from, and
// what a machine looks like to the machine that includes it.
//
// A header `alias machine "<reference>"` names another machine. The reference
// is an opaque string: the host resolves it through one interface,
// MachineSource. The parser reads the referenced notation, resolves it on its
// own, and writes its steps into the including machine with node positions of
// their own (capdag/docs/09-machine-notation.md, §Machines in step position).

// LoadedMachine is the notation of one machine, as a MachineSource gives it.
type LoadedMachine struct {
	// Id is what the source knows this machine by. Two references that
	// resolve to one machine yield one id: it is what a circle of inclusions
	// is recognized by, and what a reference written inside this machine is
	// resolved relative to.
	Id       string
	Notation string
}

// MachineSourceError says why a reference did not yield a machine.
type MachineSourceError struct {
	// NoSource: the notation is being read with nothing to resolve references
	// against. Otherwise the source holds no machine under the reference, and
	// Details says what it found.
	NoSource bool
	Details  string
}

func (e *MachineSourceError) Error() string {
	if e.NoSource {
		return "there is nothing to resolve machine references against"
	}
	return e.Details
}

// MachineSource is where the machines named in machine headers come from.
//
// A command line reads a reference as a path relative to the including file
// (FileMachineSource); an engine reads it from its machine store.
type MachineSource interface {
	// Load returns the machine `reference` names. `including` is the id of
	// the machine whose header wrote it — nil when that machine has none
	// (notation that came from no place the source knows).
	Load(reference string, including *string) (*LoadedMachine, *MachineSourceError)
}

// NoMachineSource is the source of a host that resolves no references. A
// machine header read against it is refused.
type NoMachineSource struct{}

// Load refuses every reference.
func (NoMachineSource) Load(string, *string) (*LoadedMachine, *MachineSourceError) {
	return nil, &MachineSourceError{NoSource: true}
}

// FileMachineSource holds machines in files. A reference is a path: relative
// to the directory of the including file, or to the base when the including
// machine came from no file. A machine's id is its file's canonical path.
type FileMachineSource struct {
	base string
}

// NewFileMachineSource reads references written in notation that came from no
// file relative to `base`.
func NewFileMachineSource(base string) *FileMachineSource {
	return &FileMachineSource{base: base}
}

// FileMachineId is the id a FileMachineSource gives the machine in the file
// at `path`: what to name the including machine by, so that its own
// references are read relative to it and an inclusion of it is recognized.
func FileMachineId(path string) (string, *MachineSourceError) {
	absolute, err := filepath.Abs(path)
	if err == nil {
		absolute, err = filepath.EvalSymlinks(absolute)
	}
	if err != nil {
		return "", &MachineSourceError{Details: fmt.Sprintf("%s: %v", path, err)}
	}
	return absolute, nil
}

// Load reads the file a reference names.
func (s *FileMachineSource) Load(reference string, including *string) (*LoadedMachine, *MachineSourceError) {
	directory := s.base
	if including != nil {
		directory = filepath.Dir(*including)
	}
	path := filepath.Join(directory, reference)
	id, sourceErr := FileMachineId(path)
	if sourceErr != nil {
		return nil, sourceErr
	}
	notation, err := os.ReadFile(path)
	if err != nil {
		return nil, &MachineSourceError{Details: fmt.Sprintf("%s: %v", path, err)}
	}
	return &LoadedMachine{Id: id, Notation: string(notation)}, nil
}

// MapMachineSource holds machines in memory, each under the reference that
// names it. A reference names the same machine wherever it is written. For a
// host that loads the machines a notation refers to before it parses, and for
// tests.
type MapMachineSource struct {
	machines map[string]string
}

// NewMapMachineSource returns a source that holds nothing yet.
func NewMapMachineSource() *MapMachineSource {
	return &MapMachineSource{machines: map[string]string{}}
}

// Insert holds `notation` under `reference`.
func (s *MapMachineSource) Insert(reference, notation string) {
	s.machines[reference] = notation
}

// Load returns the machine held under a reference.
func (s *MapMachineSource) Load(reference string, _ *string) (*LoadedMachine, *MachineSourceError) {
	notation, held := s.machines[reference]
	if !held {
		return nil, &MachineSourceError{Details: fmt.Sprintf("no machine is held under \"%s\"", reference)}
	}
	return &LoadedMachine{Id: reference, Notation: notation}, nil
}

// includedValue is what a member of an included step gives its arg: a node of
// the included machine, by the name its notation wrote, or a constant.
type includedValue struct {
	isConstant bool
	node       string
	constant   any
}

// namedMedia is a node of an included machine by the name its notation wrote,
// with the media it holds.
type namedMedia struct {
	name  string
	media *urn.MediaUrn
}

// includedStep is one step of an included machine, with every member under
// the name of the arg its own resolution bound it to and every target under
// the name of the output it takes. Written into another machine, it binds
// what it bound alone: nothing about where it is included can change it.
type includedStep struct {
	capUrn *urn.CapUrn
	policy StepPolicy
	// members: the arg's name and what it is given.
	members []includedMember
	// targets: the output's name and the node that takes it.
	targets []includedTarget
	// includedAs is the machine steps this step was already included through
	// inside the included machine, outermost first.
	includedAs []string
}

type includedMember struct {
	arg   string
	value includedValue
}

type includedTarget struct {
	output string
	node   string
}

// includedMachine is a machine as the machine that includes it sees it: its
// inputs and outputs, by the names its notation wrote, and its steps.
type includedMachine struct {
	// inputs is every input anchor, by name, with the media it holds. In the
	// order of the machine's strands, by name within one.
	inputs  []namedMedia
	outputs []namedMedia
	// nodeMedia is every node, by name, with the media it holds in the
	// machine as it resolved alone.
	nodeMedia []namedMedia
	steps     []includedStep
}

// includedMachineOf reads a resolved machine, with the names its notation
// wrote.
func includedMachineOf(machine *Machine, names []StrandNodeNames) *includedMachine {
	included := &includedMachine{}
	for strandIndex, strand := range machine.Strands() {
		nameOf := make(map[NodeId]string, len(names[strandIndex]))
		for name, id := range names[strandIndex] {
			nameOf[id] = name
		}
		name := func(id NodeId) string {
			written, named := nameOf[id]
			if !named {
				panic("the parser names every node of a strand it resolved")
			}
			return written
		}
		anchors := func(ids []NodeId) []namedMedia {
			held := make([]namedMedia, len(ids))
			for i, id := range ids {
				held[i] = namedMedia{name: name(id), media: strand.NodeUrn(id)}
			}
			sort.SliceStable(held, func(a, b int) bool { return held[a].name < held[b].name })
			return held
		}
		every := make([]NodeId, len(strand.Nodes()))
		for i := range every {
			every[i] = NodeId(i)
		}
		included.nodeMedia = append(included.nodeMedia, anchors(every)...)
		included.inputs = append(included.inputs, anchors(strand.InputAnchorIds())...)
		included.outputs = append(included.outputs, anchors(strand.OutputAnchorIds())...)
		for _, edge := range strand.Edges() {
			step := includedStep{
				capUrn:     edge.CapUrn,
				policy:     edge.Policy,
				includedAs: append([]string(nil), edge.IncludedAs...),
			}
			for _, binding := range edge.Assignment {
				value := includedValue{}
				if binding.Source.IsConstant {
					value.isConstant = true
					value.constant = binding.Source.Constant
				} else {
					value.node = name(binding.Source.Node)
				}
				step.members = append(step.members, includedMember{arg: binding.ArgName, value: value})
			}
			for _, output := range edge.Outputs {
				step.targets = append(step.targets, includedTarget{output: output.OutputName, node: name(output.Node)})
			}
			included.steps = append(included.steps, step)
		}
	}
	return included
}

func namesOf(held []namedMedia) []string {
	names := make([]string, len(held))
	for i, each := range held {
		names[i] = each.name
	}
	return names
}

func mediaNamed(held []namedMedia, name string) *urn.MediaUrn {
	for _, each := range held {
		if each.name == name {
			return each.media
		}
	}
	return nil
}

func (m *includedMachine) inputNames() []string  { return namesOf(m.inputs) }
func (m *includedMachine) outputNames() []string { return namesOf(m.outputs) }

// input returns the media the input of that name holds, or nil.
func (m *includedMachine) input(name string) *urn.MediaUrn { return mediaNamed(m.inputs, name) }

// output returns the media the output of that name holds, or nil.
func (m *includedMachine) output(name string) *urn.MediaUrn { return mediaNamed(m.outputs, name) }
