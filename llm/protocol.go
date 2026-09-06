// Package llm carries what a cartridge needs to speak to a language model.
//
// These are the canonical LLM protocol types, shared by floom-engine and the
// LLM cartridges, matching the capdag media-def schemas one-for-one. They live
// here rather than in a package that could version away from them: each type
// is one half of a definition capdag itself declares.
//
// This was capdag-cartridge-sdk-go, a separate module.
//
// Media defs:
//   - media:llm-generation-request;json;record
//   - media:llm-text-stream;ndjson
//   - media:llm-vocab-response;json;record
//   - media:llm-model-info;json;record
package llm

import (
	"encoding/json"
	"fmt"
	"strings"
)

// -----------------------------------------------------------------------------
// Request Type (optional discriminator)
// -----------------------------------------------------------------------------

// RequestType discriminates LLM operations when a single command handles
// several. Prefer distinct caps over the discriminator when possible.
type RequestType string

const (
	RequestTypeGenerate RequestType = "generate"
	RequestTypeGetVocab RequestType = "get_vocab"
	RequestTypeGetInfo  RequestType = "get_info"
)

// -----------------------------------------------------------------------------
// media:llm-generation-request;json;record
// -----------------------------------------------------------------------------

// GenerationRequest is the input for every LLM cap.
type GenerationRequest struct {
	Prompt    string `json:"prompt"`
	ModelSpec string `json:"model_spec"`

	RequestType      *RequestType    `json:"request_type,omitempty"`
	SystemPrompt     *string         `json:"system_prompt,omitempty"`
	RequestID        *string         `json:"request_id,omitempty"`
	MaxTokens        *uint64         `json:"max_tokens,omitempty"`
	Temperature      *float32        `json:"temperature,omitempty"`
	TopK             *int32          `json:"top_k,omitempty"`
	TopP             *float32        `json:"top_p,omitempty"`
	MinP             *float32        `json:"min_p,omitempty"`
	Seed             *uint32         `json:"seed,omitempty"`
	Grammar          *string         `json:"grammar,omitempty"`
	JSONSchema       json.RawMessage `json:"json_schema,omitempty"`
	Constraint       *ConstraintSpec `json:"constraint,omitempty"`
	ChatTemplate     *string         `json:"chat_template,omitempty"`
	StopSequences    []string        `json:"stop_sequences,omitempty"`
	MaxContextLength *uint32         `json:"max_context_length,omitempty"`
	BatchSize        *uint32         `json:"batch_size,omitempty"`
	RopeFreqBase     *float32        `json:"rope_freq_base,omitempty"`
	RopeFreqScale    *float32        `json:"rope_freq_scale,omitempty"`
	RepeatPenalty    *float32        `json:"repeat_penalty,omitempty"`
}

// NewGenerationRequestWithDefaults builds a request populated with the system
// defaults for sampling parameters.
func NewGenerationRequestWithDefaults(prompt, modelSpec string) *GenerationRequest {
	rt := RequestTypeGenerate
	return &GenerationRequest{
		Prompt:           prompt,
		ModelSpec:        modelSpec,
		RequestType:      &rt,
		MaxTokens:        u64(512),
		Temperature:      f32(0.7),
		TopK:             i32(40),
		TopP:             f32(0.9),
		MinP:             f32(0.05),
		Seed:             u32(42),
		MaxContextLength: u32(4096),
		BatchSize:        u32(2048),
		RopeFreqBase:     f32(10000.0),
		RopeFreqScale:    f32(1.0),
		RepeatPenalty:    f32(1.1),
	}
}

// EffectiveRequestType returns the request type, defaulting to Generate when
// unset.
func (r *GenerationRequest) EffectiveRequestType() RequestType {
	if r.RequestType == nil {
		return RequestTypeGenerate
	}
	return *r.RequestType
}

// ToJSON serializes the request. Encoding cannot fail for this plain data
// type; a failure here is a logic bug and panics.
func (r *GenerationRequest) ToJSON() string {
	b, err := json.Marshal(r)
	if err != nil {
		panic(fmt.Sprintf("GenerationRequest marshal: %v", err))
	}
	return string(b)
}

// ConstraintSpec is a constraint for guided generation. The discriminator
// `type` matches the Rust enum one-for-one.
type ConstraintSpec struct {
	Type        string           `json:"type"`
	Schema      json.RawMessage  `json:"schema,omitempty"`
	Pattern     string           `json:"pattern,omitempty"`
	Grammar     string           `json:"grammar,omitempty"`
	Tools       []ToolDefinition `json:"tools,omitempty"`
	Description *string          `json:"description,omitempty"`
}

// ToolDefinition describes a tool available to constrained tool-calling.
type ToolDefinition struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	Parameters  json.RawMessage `json:"parameters"`
}

// -----------------------------------------------------------------------------
// media:llm-text-stream;ndjson
// -----------------------------------------------------------------------------

// StreamMessageType discriminates NDJSON stream messages. All variants are
// wire-protocol mandated by the media def.
type StreamMessageType string

const (
	StreamToken       StreamMessageType = "token"
	StreamStatus      StreamMessageType = "status"
	StreamComplete    StreamMessageType = "complete"
	StreamToolRequest StreamMessageType = "tool_request"
	StreamError       StreamMessageType = "error"
)

// StreamMessage is a single NDJSON line on an LLM stream.
type StreamMessage struct {
	Type StreamMessageType `json:"type"`

	// Token
	Text string `json:"text,omitempty"`

	// Status
	Operation string   `json:"operation,omitempty"`
	Details   *string  `json:"details,omitempty"`
	Progress  *float32 `json:"progress,omitempty"`

	// Complete
	GeneratedText    string `json:"generated_text,omitempty"`
	TokensGenerated  uint64 `json:"tokens_generated,omitempty"`
	PromptTokens     uint64 `json:"prompt_tokens,omitempty"`
	FinishReason     string `json:"finish_reason,omitempty"`
	GenerationTimeMs uint64 `json:"generation_time_ms,omitempty"`

	// ToolRequest
	PartialText string          `json:"partial_text,omitempty"`
	ToolName    string          `json:"tool_name,omitempty"`
	ToolArgs    json.RawMessage `json:"tool_args,omitempty"`

	// Error
	Code    string `json:"code,omitempty"`
	Message string `json:"message,omitempty"`
}

// NewToken constructs a token stream message.
func NewToken(text string) *StreamMessage {
	return &StreamMessage{Type: StreamToken, Text: text}
}

// NewComplete constructs a completion stream message.
func NewComplete(generatedText string, tokensGenerated, promptTokens uint64, finishReason string, generationTimeMs uint64) *StreamMessage {
	return &StreamMessage{
		Type:             StreamComplete,
		GeneratedText:    generatedText,
		TokensGenerated:  tokensGenerated,
		PromptTokens:     promptTokens,
		FinishReason:     finishReason,
		GenerationTimeMs: generationTimeMs,
	}
}

// NewError constructs an error stream message.
func NewError(code, message string) *StreamMessage {
	return &StreamMessage{Type: StreamError, Code: code, Message: message}
}

// ToLine serializes to a single NDJSON line (no trailing newline). Encoding
// cannot fail for this plain data type; a failure here is a logic bug.
func (m *StreamMessage) ToLine() string {
	b, err := json.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("StreamMessage marshal: %v", err))
	}
	return string(b)
}

// FromLine parses a single NDJSON line.
func FromLine(line string) (*StreamMessage, error) {
	var m StreamMessage
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		return nil, err
	}
	return &m, nil
}

// FinishReason constants match the Rust SDK.
const (
	FinishReasonStop                = "stop"
	FinishReasonLength              = "length"
	FinishReasonInterrupted         = "interrupted"
	FinishReasonConstraintSatisfied = "constraint_satisfied"
)

// -----------------------------------------------------------------------------
// media:llm-vocab-response;json;record
// -----------------------------------------------------------------------------

// VocabResponse is the response body for the vocab cap.
type VocabResponse struct {
	Vocab     []string `json:"vocab"`
	VocabSize *uint64  `json:"vocab_size,omitempty"`
}

// NewVocabResponse constructs a VocabResponse with vocab_size populated from
// the vocab slice length.
func NewVocabResponse(vocab []string) *VocabResponse {
	n := uint64(len(vocab))
	return &VocabResponse{Vocab: vocab, VocabSize: &n}
}

// ToJSON serializes the response. Failure here is a logic bug.
func (v *VocabResponse) ToJSON() string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(fmt.Sprintf("VocabResponse marshal: %v", err))
	}
	return string(b)
}

// -----------------------------------------------------------------------------
// media:llm-model-info;json;record
// -----------------------------------------------------------------------------

// ModelInfo is the response body for the model-info cap.
type ModelInfo struct {
	ModelSpec      string  `json:"model_spec"`
	VocabSize      uint64  `json:"vocab_size"`
	ContextLength  *uint64 `json:"context_length,omitempty"`
	EmbeddingDim   *uint64 `json:"embedding_dim,omitempty"`
	FileSizeBytes  *uint64 `json:"file_size_bytes,omitempty"`
	HeadCount      *uint64 `json:"head_count,omitempty"`
	LayerCount     *uint64 `json:"layer_count,omitempty"`
	SupportsChat   *bool   `json:"supports_chat,omitempty"`
	SupportsTools  *bool   `json:"supports_tools,omitempty"`
}

// ToJSON serializes the response. Failure here is a logic bug.
func (m *ModelInfo) ToJSON() string {
	b, err := json.Marshal(m)
	if err != nil {
		panic(fmt.Sprintf("ModelInfo marshal: %v", err))
	}
	return string(b)
}

// -----------------------------------------------------------------------------
// Media URNs — single source of truth
// -----------------------------------------------------------------------------

const (
	MediaLLMGenerationRequest = "media:llm-generation-request;json;record"
	MediaLLMTextStream        = "media:llm-text-stream;ndjson"
	MediaLLMVocabResponse     = "media:llm-vocab-response;json;record"
	MediaLLMModelInfoResponse = "media:llm-model-info;json;record"
)

// -----------------------------------------------------------------------------
// Cap URNs
// -----------------------------------------------------------------------------

const (
	CapLLMInferenceGGUF        = "cap:gguf;in=\"media:llm-generation-request;json;record\";llm;ml-model;llm-inference;out=\"media:llm-text-stream;ndjson\""
	CapLLMInferenceMLX         = "cap:in=\"media:llm-generation-request;json;record\";llm;ml-model;mlx;llm-inference;out=\"media:llm-text-stream;ndjson\""
	CapLLMInferenceCandle      = "cap:candle;in=\"media:llm-generation-request;json;record\";llm;ml-model;llm-inference;out=\"media:llm-text-stream;ndjson\""
	CapLLMInferenceConstrained = "cap:constrained;gguf;in=\"media:llm-generation-request;json;record\";llm;ml-model;llm-inference-constrained;out=\"media:llm-text-stream;ndjson\""
	CapLLMVocab                = "cap:llm-vocab;llm;ml-model;gguf;in=\"media:llm-generation-request;json;record\";out=\"media:llm-vocab-response;json;record\""
	CapLLMModelInfo            = "cap:llm-model-info;llm;ml-model;gguf;in=\"media:llm-generation-request;json;record\";out=\"media:llm-model-info;json;record\""
	CapGenerateEmbeddings      = "cap:generate-embeddings;ml-model;gguf;in=\"media:enc=utf-8\";out=\"media:embedding-vector;enc=utf-8;record\""
	CapEmbeddingsDimensions    = "cap:gguf;in=\"media:embeddings;enc=utf-8;gguf;model-spec;tokenizer-embedded-gguf\";ml-model;embeddings-dimensions;out=\"media:integer;model-dim;numeric\""
	CapDescribeImage           = "cap:gguf;in=\"media:ext=png;image\";ml-model;describe-image;out=\"media:enc=utf-8;ext=txt;image-description;plain-text\";vision"
)

// -----------------------------------------------------------------------------
// Model spec → backend classification
// -----------------------------------------------------------------------------

const (
	BackendGGUF   = "gguf"
	BackendMLX    = "mlx"
	BackendCandle = "candle"
)

// BackendForModelSpec classifies a model spec string into its inference
// backend. Mirrors the Rust function of the same name and is the single source
// of truth for spec → backend mapping.
func BackendForModelSpec(modelSpec string) string {
	lower := strings.ToLower(modelSpec)
	if strings.Contains(lower, "mlx-community") || strings.Contains(lower, ";mlx") {
		return BackendMLX
	}
	if strings.Contains(lower, ".gguf") || strings.Contains(lower, "gguf") {
		return BackendGGUF
	}
	return BackendCandle
}

// -----------------------------------------------------------------------------
// Internal helpers for optional-value construction.
// -----------------------------------------------------------------------------

func u64(v uint64) *uint64   { return &v }
func u32(v uint32) *uint32   { return &v }
func i32(v int32) *int32     { return &v }
func f32(v float32) *float32 { return &v }
