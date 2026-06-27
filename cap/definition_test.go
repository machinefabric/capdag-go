package cap

import (
	"encoding/json"
	"testing"

	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Test helper to create URNs with required in/out specs
func capTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:fmt=json;record"`
	}
	return `cap:in="media:void";out="media:fmt=json;record";` + tags
}

// TEST108: Test creating new cap with URN, title, and command verifies correct initialization
func Test108_cap_creation(t *testing.T) {
	id, err := urn.NewCapUrnFromString(capTestUrn("transform;format=json;data_processing"))
	require.NoError(t, err)

	cap := NewCap(id, "Transform JSON Data", "test-command")

	// Check that URN string contains the expected tags
	urnStr := cap.UrnString()
	assert.Contains(t, urnStr, "transform")
	assert.Contains(t, urnStr, "in=")
	assert.Contains(t, urnStr, "media:void")
	assert.Contains(t, urnStr, "out=")
	assert.Contains(t, urnStr, "record")
	assert.Equal(t, "Transform JSON Data", cap.Title)
	assert.NotNil(t, cap.Metadata)
	assert.Empty(t, cap.Metadata)
}

// TEST109: Test creating cap with metadata initializes and retrieves metadata correctly
func Test109_cap_with_metadata(t *testing.T) {
	id, err := urn.NewCapUrnFromString(capTestUrn("arithmetic;compute;subtype=math"))
	require.NoError(t, err)

	metadata := map[string]string{
		"precision":  "double",
		"operations": "add,subtract,multiply,divide",
	}

	cap := NewCapWithMetadata(id, "Perform Mathematical Operations", "test-command", metadata)

	assert.Equal(t, "Perform Mathematical Operations", cap.Title)

	precision, exists := cap.GetMetadata("precision")
	assert.True(t, exists)
	assert.Equal(t, "double", precision)

	operations, exists := cap.GetMetadata("operations")
	assert.True(t, exists)
	assert.Equal(t, "add,subtract,multiply,divide", operations)

	assert.True(t, cap.HasMetadata("precision"))
	assert.False(t, cap.HasMetadata("nonexistent"))
}

// TEST110: Test cap matching with subset semantics for request fulfillment
func Test110_cap_matching(t *testing.T) {
	// Use type=data_processing key-value instead of flag for proper matching
	id, err := urn.NewCapUrnFromString(capTestUrn("transform;format=json;type=data_processing"))
	require.NoError(t, err)

	cap := NewCap(id, "Transform JSON Data", "test-command")

	assert.True(t, cap.MatchesRequest(capTestUrn("transform;format=json;type=data_processing")))
	assert.True(t, cap.MatchesRequest(capTestUrn("transform;format=*;type=data_processing")))
	assert.True(t, cap.MatchesRequest(capTestUrn("type=data_processing")))
	assert.False(t, cap.MatchesRequest(capTestUrn("type=compute")))
}

// TEST111: Test getting and setting cap title updates correctly
func Test111_cap_title(t *testing.T) {
	id, err := urn.NewCapUrnFromString(capTestUrn("extract;target=metadata"))
	require.NoError(t, err)

	cap := NewCap(id, "Extract Document Metadata", "extract-metadata")

	assert.Equal(t, "Extract Document Metadata", cap.GetTitle())
	assert.Equal(t, "Extract Document Metadata", cap.Title)

	cap.SetTitle("Extract File Metadata")
	assert.Equal(t, "Extract File Metadata", cap.GetTitle())
	assert.Equal(t, "Extract File Metadata", cap.Title)
}

// TEST112: Test cap equality based on URN and title matching
func Test112_cap_definition_equality(t *testing.T) {
	id1, err := urn.NewCapUrnFromString(capTestUrn("transform;format=json"))
	require.NoError(t, err)
	id2, err := urn.NewCapUrnFromString(capTestUrn("transform;format=json"))
	require.NoError(t, err)

	cap1 := NewCap(id1, "Transform JSON Data", "transform")
	cap2 := NewCap(id2, "Transform JSON Data", "transform")
	cap3 := NewCap(id2, "Convert JSON Format", "transform")

	assert.True(t, cap1.Equals(cap2))
	assert.False(t, cap1.Equals(cap3))
	assert.False(t, cap2.Equals(cap3))
}

// TEST113: Test cap stdin support via args with stdin source and serialization roundtrip
func Test113_cap_stdin(t *testing.T) {
	id, err := urn.NewCapUrnFromString(capTestUrn("generate;target=embeddings"))
	require.NoError(t, err)

	cap := NewCap(id, "Generate Embeddings", "generate")

	// By default, caps should not accept stdin
	assert.False(t, cap.AcceptsStdin())
	assert.Nil(t, cap.GetStdinMediaUrn())

	// Enable stdin support by adding an arg with a stdin source
	stdinUrn := "media:enc=utf-8"
	inputTextDesc := "Input text"
	stdinArg := CapArg{
		MediaUrn:       "media:enc=utf-8",
		Required:       true,
		Sources:        []ArgSource{{Stdin: &stdinUrn}},
		ArgDescription: &inputTextDesc,
	}
	cap.AddArg(stdinArg)

	assert.True(t, cap.AcceptsStdin())
	assert.Equal(t, "media:enc=utf-8", *cap.GetStdinMediaUrn())

	// Test serialization/deserialization preserves the args
	serialized, err := json.Marshal(cap)
	require.NoError(t, err)
	assert.Contains(t, string(serialized), `"args"`)
	assert.Contains(t, string(serialized), `"stdin"`)

	var deserialized Cap
	err = json.Unmarshal(serialized, &deserialized)
	require.NoError(t, err)
	assert.True(t, deserialized.AcceptsStdin())
	assert.Equal(t, "media:enc=utf-8", *deserialized.GetStdinMediaUrn())
}

// TEST114: Test ArgSource type variants stdin, position, and cli_flag with their accessors
func Test114_arg_source_types(t *testing.T) {
	// Test stdin source
	stdinUrn := "media:text"
	stdinSource := ArgSource{Stdin: &stdinUrn}
	assert.Equal(t, "stdin", stdinSource.GetType())
	assert.NotNil(t, stdinSource.StdinMediaUrn())
	assert.Equal(t, "media:text", *stdinSource.StdinMediaUrn())
	assert.Nil(t, stdinSource.GetPosition())
	assert.Nil(t, stdinSource.GetCliFlag())

	// Test position source
	pos := 0
	positionSource := ArgSource{Position: &pos}
	assert.Equal(t, "position", positionSource.GetType())
	assert.Nil(t, positionSource.StdinMediaUrn())
	assert.NotNil(t, positionSource.GetPosition())
	assert.Equal(t, 0, *positionSource.GetPosition())
	assert.Nil(t, positionSource.GetCliFlag())

	// Test cli_flag source
	flag := "--input"
	cliFlagSource := ArgSource{CliFlag: &flag}
	assert.Equal(t, "cli_flag", cliFlagSource.GetType())
	assert.Nil(t, cliFlagSource.StdinMediaUrn())
	assert.Nil(t, cliFlagSource.GetPosition())
	assert.NotNil(t, cliFlagSource.GetCliFlag())
	assert.Equal(t, "--input", *cliFlagSource.GetCliFlag())
}

// TEST115: Test CapArg serialization and deserialization with multiple sources
func Test115_cap_arg_serialization(t *testing.T) {
	flag := "--name"
	pos := 0
	desc := "The name argument"
	metadata := map[string]any{
		"kind":  "example",
		"flags": []any{true, false},
	}
	arg := CapArg{
		MediaUrn:       "media:string",
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &flag}, {Position: &pos}},
		ArgDescription: &desc,
		DefaultValue:   400,
		Metadata:       metadata,
	}

	serialized, err := json.Marshal(arg)
	require.NoError(t, err)
	jsonStr := string(serialized)

	assert.Contains(t, jsonStr, `"media_urn":"media:string"`)
	assert.Contains(t, jsonStr, `"required":true`)
	assert.Contains(t, jsonStr, `"cli_flag":"--name"`)
	assert.Contains(t, jsonStr, `"position":0`)
	assert.Contains(t, jsonStr, `"default_value":400`)

	var deserialized CapArg
	err = json.Unmarshal(serialized, &deserialized)
	require.NoError(t, err)
	assert.Equal(t, arg.MediaUrn, deserialized.MediaUrn)
	assert.Equal(t, arg.Required, deserialized.Required)
	require.NotNil(t, deserialized.ArgDescription)
	assert.Equal(t, *arg.ArgDescription, *deserialized.ArgDescription)
	assert.Equal(t, json.Number("400"), deserialized.DefaultValue)
	assert.Equal(t, map[string]any{
		"kind":  "example",
		"flags": []any{true, false},
	}, deserialized.Metadata)
}

// TEST116: Test CapArg constructor methods basic and with_description create args correctly
func Test116_cap_arg_constructors(t *testing.T) {
	// Test basic constructor
	flag := "--name"
	arg := NewCapArg("media:string", true, []ArgSource{{CliFlag: &flag}})
	assert.Equal(t, "media:string", arg.MediaUrn)
	assert.True(t, arg.Required)
	assert.Len(t, arg.Sources, 1)
	assert.Nil(t, arg.ArgDescription)
	assert.Nil(t, arg.DefaultValue)

	// Test with description
	pos := 0
	arg2 := NewCapArgWithDescription(
		"media:integer",
		false,
		[]ArgSource{{Position: &pos}},
		"The count argument",
	)
	assert.Equal(t, "media:integer", arg2.MediaUrn)
	assert.False(t, arg2.Required)
	require.NotNil(t, arg2.ArgDescription)
	assert.Equal(t, "The count argument", *arg2.ArgDescription)
}

// Helper matching Rust's test_urn (with in="media:void";out="media:record")
func defTestUrn(tags string) string {
	if tags == "" {
		return `cap:in="media:void";out="media:record"`
	}
	return `cap:in="media:void";out="media:record";` + tags
}

// TEST591: is_more_specific_than returns true when self has more tags for same request
func Test591_is_more_specific_than(t *testing.T) {
	generalUrn, err := urn.NewCapUrnFromString(defTestUrn("transform"))
	require.NoError(t, err)
	general := NewCap(generalUrn, "General", "cmd")

	specificUrn, err := urn.NewCapUrnFromString(defTestUrn("transform;format=json"))
	require.NoError(t, err)
	specific := NewCap(specificUrn, "Specific", "cmd")

	unrelatedUrn, err := urn.NewCapUrnFromString(defTestUrn("convert"))
	require.NoError(t, err)
	unrelated := NewCap(unrelatedUrn, "Unrelated", "cmd")

	// Specific is more specific than general for the general request
	assert.True(t, specific.IsMoreSpecificThan(general, defTestUrn("transform")),
		"specific cap must be more specific than general")
	assert.False(t, general.IsMoreSpecificThan(specific, defTestUrn("transform")),
		"general cap must not be more specific than specific")

	// If either doesn't accept the request, returns false
	assert.False(t, general.IsMoreSpecificThan(unrelated, defTestUrn("transform")),
		"unrelated cap doesn't accept request, so no comparison possible")
}

// TEST592: remove_metadata adds then removes metadata correctly
func Test592_remove_metadata(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("test"))
	require.NoError(t, err)
	c := NewCap(u, "Test", "cmd")

	c.SetMetadata("key1", "val1")
	c.SetMetadata("key2", "val2")
	assert.True(t, c.HasMetadata("key1"))
	assert.True(t, c.HasMetadata("key2"))

	removed, ok := c.RemoveMetadata("key1")
	assert.True(t, ok)
	assert.Equal(t, "val1", removed)
	assert.False(t, c.HasMetadata("key1"))
	assert.True(t, c.HasMetadata("key2"))

	// Removing non-existent returns false
	_, ok = c.RemoveMetadata("nonexistent")
	assert.False(t, ok)
}

// TEST593: registered_by lifecycle — set, get, clear
func Test593_registered_by_lifecycle(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("test"))
	require.NoError(t, err)
	c := NewCap(u, "Test", "cmd")

	// Initially nil
	assert.Nil(t, c.GetRegisteredBy())

	// Set
	reg := NewRegisteredBy("alice", "2026-02-19T10:00:00Z")
	c.SetRegisteredBy(&reg)
	got := c.GetRegisteredBy()
	require.NotNil(t, got)
	assert.Equal(t, "alice", got.Username)
	assert.Equal(t, "2026-02-19T10:00:00Z", got.RegisteredAt)

	// Clear
	c.ClearRegisteredBy()
	assert.Nil(t, c.GetRegisteredBy())
}

// TEST594: metadata_json lifecycle — set, get, clear
func Test594_metadata_json_lifecycle(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("test"))
	require.NoError(t, err)
	c := NewCap(u, "Test", "cmd")

	// Initially nil
	assert.Nil(t, c.GetMetadataJSON())

	// Set
	jsonData := map[string]any{"version": 2, "tags": []string{"experimental"}}
	c.SetMetadataJSON(jsonData)
	assert.Equal(t, jsonData, c.GetMetadataJSON())

	// Clear
	c.ClearMetadataJSON()
	assert.Nil(t, c.GetMetadataJSON())
}

// TEST595: with_args constructor stores args correctly
func Test595_with_args_constructor(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("test"))
	require.NoError(t, err)
	pos := 0
	flag := "--count"
	args := []CapArg{
		NewCapArg("media:string", true, []ArgSource{{Position: &pos}}),
		NewCapArg("media:integer", false, []ArgSource{{CliFlag: &flag}}),
	}

	c := NewCapWithArgs(u, "Test", "cmd", args)
	assert.Len(t, c.GetArgs(), 2)
	assert.Equal(t, "media:string", c.GetArgs()[0].MediaUrn)
	assert.True(t, c.GetArgs()[0].Required)
	assert.Equal(t, "media:integer", c.GetArgs()[1].MediaUrn)
	assert.False(t, c.GetArgs()[1].Required)
}

// TEST596: with_full_definition constructor stores all fields
func Test596_with_full_definition_constructor(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("test"))
	require.NoError(t, err)
	metadata := map[string]string{"env": "prod"}
	args := []CapArg{NewCapArg("media:string", true, nil)}
	output := NewCapOutput("media:object", "Output object")
	jsonMeta := map[string]any{"v": 1}
	desc := "Description"

	c := NewCapWithFullDefinition(
		u, "Full Cap", &desc, metadata, "full-cmd",
		args, output, jsonMeta,
	)

	assert.Equal(t, "Full Cap", c.Title)
	require.NotNil(t, c.CapDescription)
	assert.Equal(t, "Description", *c.CapDescription)
	val, ok := c.GetMetadata("env")
	assert.True(t, ok)
	assert.Equal(t, "prod", val)
	assert.Equal(t, "full-cmd", c.GetCommand())
	assert.Len(t, c.GetArgs(), 1)
	require.NotNil(t, c.GetOutput())
	assert.Equal(t, "media:object", c.GetOutput().MediaUrn)
	assert.Equal(t, jsonMeta, c.GetMetadataJSON())
	// registered_by is not set by NewCapWithFullDefinition
	assert.Nil(t, c.GetRegisteredBy())
}

// TEST597: CapArg::with_full_definition stores all fields including optional ones
func Test597_cap_arg_with_full_definition(t *testing.T) {
	defaultVal := map[string]any{"chunk_size": 400, "timestamps": false}
	meta := map[string]any{"hint": "enter name"}
	flag := "--name"

	arg := NewCapArgWithFullDefinition(
		"media:string", true,
		[]ArgSource{{CliFlag: &flag}},
		"User name", defaultVal, meta,
	)

	assert.Equal(t, "media:string", arg.MediaUrn)
	assert.True(t, arg.Required)
	require.NotNil(t, arg.ArgDescription)
	assert.Equal(t, "User name", *arg.ArgDescription)
	assert.Equal(t, defaultVal, arg.DefaultValue)
	assert.Equal(t, meta, arg.GetMetadata())

	// Metadata lifecycle
	arg.ClearMetadata()
	assert.Nil(t, arg.GetMetadata())
	arg.SetMetadata("new")
	assert.Equal(t, "new", arg.GetMetadata())
}

// TEST598: CapOutput lifecycle — set_output, set/clear metadata
func Test598_cap_output_lifecycle(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("test"))
	require.NoError(t, err)
	c := NewCap(u, "Test", "cmd")

	// Initially no output
	assert.Nil(t, c.GetOutput())

	// Set output
	output := NewCapOutput("media:string", "Text output")
	output.SetMetadata(map[string]any{"format": "plain"})
	c.SetOutput(output)

	got := c.GetOutput()
	require.NotNil(t, got)
	assert.Equal(t, "media:string", got.MediaUrn)
	assert.Equal(t, "Text output", got.OutputDescription)
	assert.NotNil(t, got.GetMetadata())

	// CapOutput with_full_definition
	output2 := NewCapOutputWithFullDefinition("media:json", "JSON output", map[string]any{"v": 2})
	assert.Equal(t, "media:json", output2.MediaUrn)
	assert.NotNil(t, output2.GetMetadata())

	// Clear metadata on output
	output2.ClearMetadata()
	assert.Nil(t, output2.GetMetadata())
}

// Additional existing tests below (not part of TEST108-116 sequence)

func Test6238_CapRequestHandling(t *testing.T) {
	id, err := urn.NewCapUrnFromString(capTestUrn("extract;target=metadata"))
	require.NoError(t, err)

	cap1 := NewCap(id, "Extract Metadata", "extract-cmd")
	cap2 := NewCap(id, "Extract Metadata", "extract-cmd")

	assert.True(t, cap1.AcceptsRequest(cap2.Urn))

	otherId, err := urn.NewCapUrnFromString(capTestUrn("generate;image"))
	require.NoError(t, err)
	cap3 := NewCap(otherId, "Generate Image", "generate-cmd")

	assert.False(t, cap1.AcceptsRequest(cap3.Urn))
}

// TEST6245: Cap description
func Test6245_CapDescription(t *testing.T) {
	id, err := urn.NewCapUrnFromString(capTestUrn("parse;format=json;data"))
	require.NoError(t, err)

	cap1 := NewCapWithDescription(id, "Parse JSON Data", "parse-cmd", "Parse JSON data")
	cap2 := NewCapWithDescription(id, "Parse JSON Data", "parse-cmd", "Parse JSON data v2")
	cap3 := NewCapWithDescription(id, "Parse JSON Data", "parse-cmd", "Parse JSON data")

	assert.False(t, cap1.Equals(cap2)) // Different descriptions
	assert.True(t, cap1.Equals(cap3))  // Same everything
}

// TEST6248: Cap with media defs
func Test6248_CapWithMediaDefs(t *testing.T) {
	// Use proper in/out in the URN - custom media URN in out
	id, err := urn.NewCapUrnFromString(`cap:in="media:string";query;out="media:result";target=structured`)
	require.NoError(t, err)

	cap := NewCap(id, "Query Structured Data", "query-cmd")

	// Get test registry and seed the specs the cap references.
	registry := testRegistry(t)
	registry.AddSpec((media.NewMediaDef(standard.MediaString, "text/plain", media.ProfileStr)).ToStored())
	registry.AddSpec((media.NewMediaDefWithSchema(
		"media:result",
		"application/json",
		"https://example.com/schema/result",
		map[string]any{
			"type": "object",
			"properties": map[string]any{
				"data": map[string]any{"type": "string"},
			},
		},
	)).ToStored())

	// Add an argument using the media URN with new architecture
	cliFlag := "--query"
	pos := 0
	queryDesc := "The query string"
	cap.AddArg(CapArg{
		MediaUrn:       standard.MediaString,
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: &queryDesc,
	})

	// Add output
	cap.SetOutput(NewCapOutput("media:result", "Query result"))

	// Resolve the argument spec
	args := cap.GetArgs()
	require.Len(t, args, 1)
	arg := args[0]
	resolved, err := arg.Resolve(registry)
	require.NoError(t, err)
	assert.Equal(t, "text/plain", resolved.MediaType)
	assert.Equal(t, media.ProfileStr, resolved.ProfileURI)

	// Resolve the output spec
	outResolved, err := cap.Output.Resolve(registry)
	require.NoError(t, err)
	assert.Equal(t, "application/json", outResolved.MediaType)
	assert.NotNil(t, outResolved.Schema)
}

// TEST1127: Documentation field round-trips through JSON serialize/deserialize.
// The body must survive multi-line markdown with CRLF, backticks, double quotes,
// and Unicode characters — every character must be preserved.
func Test1127_cap_documentation_round_trip_with_markdown_body(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("documented"))
	require.NoError(t, err)
	c := NewCap(u, "Documented Cap", "documented")

	body := "# Documented Cap\r\n\nDoes the thing.\n\n```bash\necho \"hi\"\n```\n\nSee also: ★\n"
	c.SetDocumentation(body)
	require.Equal(t, body, *c.GetDocumentation())

	data, err := json.Marshal(c)
	require.NoError(t, err)
	require.Contains(t, string(data), `"documentation"`,
		"documentation field must be present in JSON output")

	var restored Cap
	require.NoError(t, json.Unmarshal(data, &restored))
	require.NotNil(t, restored.GetDocumentation(), "documentation must survive round-trip")
	assert.Equal(t, body, *restored.GetDocumentation(), "documentation body must not be mutated during round-trip")
}

// TEST6686: When Documentation is nil, the serializer must omit the field entirely.
// There must be no "documentation":null — only absence.
func Test6686_cap_documentation_omitted_when_none(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("undocumented"))
	require.NoError(t, err)
	c := NewCap(u, "Undocumented Cap", "undocumented")
	require.Nil(t, c.GetDocumentation())

	data, err := json.Marshal(c)
	require.NoError(t, err)
	assert.NotContains(t, string(data), "documentation",
		"documentation field must be omitted when nil, got: %s", string(data))

	var restored Cap
	require.NoError(t, json.Unmarshal(data, &restored))
	assert.Nil(t, restored.GetDocumentation())
}

// TEST6687: A capfab-shaped JSON document with a documentation field
// must deserialize into a Cap with the body intact.
func Test6687_cap_documentation_parses_from_capfab_json(t *testing.T) {
	raw := `{
		"urn": "cap:in=\"media:enc=utf-8\";docparse;out=\"media:enc=utf-8\"",
		"title": "Doc Parse",
		"command": "docparse",
		"cap_description": "short",
		"documentation": "## Heading\n\nbody text",
		"metadata": {}
	}`
	var c Cap
	require.NoError(t, json.Unmarshal([]byte(raw), &c), "must parse capfab-shaped JSON")
	require.NotNil(t, c.GetDocumentation())
	assert.Equal(t, "## Heading\n\nbody text", *c.GetDocumentation())
	assert.Equal(t, "short", *c.CapDescription)
}

// TEST6688: Documentation set/clear lifecycle must not cross-contaminate cap_description.
func Test6688_cap_documentation_set_and_clear_lifecycle(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("lifecycle"))
	require.NoError(t, err)
	short := "short"
	c := &Cap{Urn: u, Title: "Lifecycle", Command: "lifecycle", CapDescription: &short}

	assert.Equal(t, "short", *c.CapDescription)
	assert.Nil(t, c.GetDocumentation())

	c.SetDocumentation("long body")
	assert.Equal(t, "long body", *c.GetDocumentation())
	// setter must not touch cap_description
	assert.Equal(t, "short", *c.CapDescription)

	c.ClearDocumentation()
	assert.Nil(t, c.GetDocumentation())
	// clearer must not touch cap_description
	assert.Equal(t, "short", *c.CapDescription)
}

// TEST6251: Cap.Version zero value is omitted on serialize and defaults to 0 on deserialize.
func Test6251_cap_version_zero_round_trip(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("versioned"))
	require.NoError(t, err)
	c := NewCap(u, "Versioned Cap", "versioned")
	require.Equal(t, uint32(0), c.Version, "new cap must have Version 0")

	data, err := json.Marshal(c)
	require.NoError(t, err)
	assert.NotContains(t, string(data), `"version"`,
		"version must be omitted when 0, got: %s", string(data))

	var restored Cap
	require.NoError(t, json.Unmarshal(data, &restored))
	assert.Equal(t, uint32(0), restored.Version, "deserialized cap must have Version 0 when field absent")
}

// TEST6255: Cap.Version non-zero value round-trips through JSON serialize/deserialize.
func Test6255_cap_version_nonzero_round_trip(t *testing.T) {
	u, err := urn.NewCapUrnFromString(defTestUrn("versioned"))
	require.NoError(t, err)
	c := NewCap(u, "Versioned Cap", "versioned")
	c.Version = 3

	data, err := json.Marshal(c)
	require.NoError(t, err)
	assert.Contains(t, string(data), `"version":3`,
		"version must be present when non-zero, got: %s", string(data))

	var restored Cap
	require.NoError(t, json.Unmarshal(data, &restored))
	assert.Equal(t, uint32(3), restored.Version, "deserialized cap must have Version 3")
}

// TEST6259: Cap j s o n round trip
func Test6259_CapJSONRoundTrip(t *testing.T) {
	id, err := urn.NewCapUrnFromString(capTestUrn("test"))
	require.NoError(t, err)

	cap := NewCap(id, "Test Cap", "test-command")
	cliFlag := "--input"
	pos := 0
	inputTextDesc2 := "Input text"
	cap.AddArg(CapArg{
		MediaUrn:       standard.MediaString,
		Required:       true,
		Sources:        []ArgSource{{CliFlag: &cliFlag}, {Position: &pos}},
		ArgDescription: &inputTextDesc2,
	})
	cap.SetOutput(NewCapOutput(standard.MediaJSON, "Output object"))

	// Serialize to JSON
	jsonData, err := json.Marshal(cap)
	require.NoError(t, err)

	// Deserialize
	var deserialized Cap
	err = json.Unmarshal(jsonData, &deserialized)
	require.NoError(t, err)

	// Verify key fields
	assert.Equal(t, cap.Title, deserialized.Title)
	assert.Equal(t, cap.Command, deserialized.Command)
	assert.Equal(t, len(cap.GetArgs()), len(deserialized.GetArgs()))
	assert.Equal(t, cap.GetArgs()[0].MediaUrn, deserialized.GetArgs()[0].MediaUrn)
	assert.Equal(t, cap.Output.MediaUrn, deserialized.Output.MediaUrn)
}
