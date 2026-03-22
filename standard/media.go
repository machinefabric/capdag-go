// Package standard provides standard media URN constants and cap URN builders
package standard

// =============================================================================
// STANDARD MEDIA URN CONSTANTS
// =============================================================================
//
// Cardinality and Structure use orthogonal marker tags:
// - `list` marker: presence = list/array, absence = scalar (default)
// - `record` marker: presence = has internal fields, absence = opaque (default)
//
// Examples:
// - `media:pdf` → scalar, opaque (no markers)
// - `media:textable;list` → list, opaque (has list marker)
// - `media:json;textable;record` → scalar, record (has record marker)
// - `media:json;list;record;textable` → list of records (has both markers)

// Primitive types - URNs must match base.toml definitions

// MediaVoid is the media URN for void (no input/output) - no coercion tags
const MediaVoid = "media:void"

// MediaString is the media URN for string type - textable (can become text), scalar by default (no list marker)
const MediaString = "media:textable"

// MediaInteger is the media URN for integer type - textable, numeric (math ops valid), scalar by default
const MediaInteger = "media:integer;textable;numeric"

// MediaNumber is the media URN for number type - textable, numeric, scalar by default
const MediaNumber = "media:textable;numeric"

// MediaBoolean is the media URN for boolean type - uses "bool" not "boolean" per base.toml
const MediaBoolean = "media:bool;textable"

// MediaObject is the media URN for a generic record/object type - has internal key-value structure but NOT textable
// Use MediaJSON for textable JSON objects.
const MediaObject = "media:record"

// MediaIdentity is the media URN for binary data - the most general media type (no constraints)
const MediaIdentity = "media:"

// Array types - URNs must match base.toml definitions

// MediaStringArray is the media URN for string array type - textable with list marker
const MediaStringArray = "media:list;textable"

// MediaIntegerArray is the media URN for integer array type - textable, numeric with list marker
const MediaIntegerArray = "media:integer;list;textable;numeric"

// MediaNumberArray is the media URN for number array type - textable, numeric with list marker
const MediaNumberArray = "media:list;textable;numeric"

// MediaBooleanArray is the media URN for boolean array type - uses "bool" with list marker
const MediaBooleanArray = "media:bool;list;textable"

// MediaObjectArray is the media URN for object array type - list of records (NOT textable)
// Use a specific format like JSON array for textable object arrays.
const MediaObjectArray = "media:list;record"

// Semantic media types for specialized content

// MediaPNG is the media URN for PNG image data
const MediaPNG = "media:image;png"

// MediaAudio is the media URN for audio data (wav, mp3, flac, etc.)
const MediaAudio = "media:wav;audio"

// MediaVideo is the media URN for video data (mp4, webm, mov, etc.)
const MediaVideo = "media:video"

// Semantic AI input types - distinguished by their purpose/context

// MediaAudioSpeech is the media URN for audio input containing speech for transcription (Whisper)
const MediaAudioSpeech = "media:audio;wav;speech"

// MediaImageThumbnail is the media URN for thumbnail image output
const MediaImageThumbnail = "media:image;png;thumbnail"

// Document types (PRIMARY naming - type IS the format)

// MediaPDF is the media URN for PDF documents
const MediaPDF = "media:pdf"

// MediaEPUB is the media URN for EPUB documents
const MediaEPUB = "media:epub"

// Text format types (PRIMARY naming - type IS the format)

// MediaMarkdown is the media URN for Markdown text
const MediaMarkdown = "media:md;textable"

// MediaTXT is the media URN for plain text
const MediaTXT = "media:txt;textable"

// MediaRST is the media URN for reStructuredText
const MediaRST = "media:rst;textable"

// MediaLog is the media URN for log files
const MediaLog = "media:log;textable"

// MediaHTML is the media URN for HTML documents
const MediaHTML = "media:html;textable"

// MediaXML is the media URN for XML documents
const MediaXML = "media:xml;textable"

// MediaJSON is the media URN for JSON data - has record marker (structured key-value)
const MediaJSON = "media:json;record;textable"

// MediaJSONSchema is the media URN for JSON with schema constraint (input for structured queries)
const MediaJSONSchema = "media:json;json-schema;record;textable"

// MediaYAML is the media URN for YAML data - has record marker (structured key-value)
const MediaYAML = "media:record;textable;yaml"

// File path types - for arguments that represent filesystem paths

// MediaFilePath is the media URN for a single file path - textable, scalar by default (no list marker)
const MediaFilePath = "media:file-path;textable"

// MediaFilePathArray is the media URN for an array of file paths - textable with list marker
const MediaFilePathArray = "media:file-path;list;textable"

// Semantic text input types - distinguished by their purpose/context

// MediaFrontmatterText is the media URN for frontmatter text (book metadata) - scalar by default
const MediaFrontmatterText = "media:frontmatter;textable"

// MediaModelSpec is the media URN for model spec (provider:model format, HuggingFace name, etc.) - scalar by default
// Generic, backend-agnostic — used by modelcartridge for download/status/path operations.
const MediaModelSpec = "media:model-spec;textable"

// Backend + use-case specific model-spec variants.
// Each inference cap declares the variant matching its backend and purpose,
// so slot values can target a specific cartridge+task without ambiguity.

// GGUF backend

// MediaModelSpecGGUFVision is the GGUF vision model spec (e.g. moondream2)
const MediaModelSpecGGUFVision = "media:model-spec;gguf;textable;vision"

// MediaModelSpecGGUFLLM is the GGUF LLM model spec (e.g. Mistral-7B)
const MediaModelSpecGGUFLLM = "media:model-spec;gguf;textable;llm"

// MediaModelSpecGGUFEmbeddings is the GGUF embeddings model spec (e.g. nomic-embed)
const MediaModelSpecGGUFEmbeddings = "media:model-spec;gguf;textable;embeddings"

// MLX backend

// MediaModelSpecMLXVision is the MLX vision model spec (e.g. Qwen3-VL)
const MediaModelSpecMLXVision = "media:model-spec;mlx;textable;vision"

// MediaModelSpecMLXLLM is the MLX LLM model spec (e.g. Llama-3.2-3B)
const MediaModelSpecMLXLLM = "media:model-spec;mlx;textable;llm"

// MediaModelSpecMLXEmbeddings is the MLX embeddings model spec (e.g. all-MiniLM-L6-v2)
const MediaModelSpecMLXEmbeddings = "media:model-spec;mlx;textable;embeddings"

// Candle backend

// MediaModelSpecCandleVision is the Candle vision model spec (e.g. BLIP)
const MediaModelSpecCandleVision = "media:model-spec;candle;textable;vision"

// MediaModelSpecCandleEmbeddings is the Candle text embeddings model spec (e.g. BERT)
const MediaModelSpecCandleEmbeddings = "media:model-spec;candle;textable;embeddings"

// MediaModelSpecCandleImageEmbeddings is the Candle image embeddings model spec (e.g. CLIP)
const MediaModelSpecCandleImageEmbeddings = "media:model-spec;candle;image-embeddings;textable"

// MediaModelSpecCandleTranscription is the Candle transcription model spec (e.g. Whisper)
const MediaModelSpecCandleTranscription = "media:model-spec;candle;textable;transcription"

// MediaMLXModelPath is the media URN for MLX model path - scalar by default
const MediaMLXModelPath = "media:mlx-model-path;textable"

// MediaModelRepo is the media URN for model repository (input for list-models) - has record marker
const MediaModelRepo = "media:model-repo;record;textable"

// CAPDAG output types - record marker for structured JSON objects, list marker for arrays

// MediaModelDim is the media URN for model dimension output - scalar by default (no list marker)
const MediaModelDim = "media:integer;model-dim;numeric;textable"

// MediaDownloadOutput is the media URN for model download output - has record marker
const MediaDownloadOutput = "media:download-result;record;textable"

// MediaListOutput is the media URN for model list output - has record marker
const MediaListOutput = "media:model-list;record;textable"

// MediaStatusOutput is the media URN for model status output - has record marker
const MediaStatusOutput = "media:model-status;record;textable"

// MediaContentsOutput is the media URN for model contents output - has record marker
const MediaContentsOutput = "media:model-contents;record;textable"

// MediaAvailabilityOutput is the media URN for model availability output - has record marker
const MediaAvailabilityOutput = "media:model-availability;record;textable"

// MediaPathOutput is the media URN for model path output - has record marker
const MediaPathOutput = "media:model-path;record;textable"

// MediaEmbeddingVector is the media URN for embedding vector output - has record marker
const MediaEmbeddingVector = "media:embedding-vector;record;textable"

// MediaLLMInferenceOutput is the media URN for LLM inference output - has record marker
const MediaLLMInferenceOutput = "media:generated-text;record;textable"

// MediaFileMetadata is the media URN for extracted metadata - has record marker
const MediaFileMetadata = "media:file-metadata;record;textable"

// MediaDocumentOutline is the media URN for extracted outline - has record marker
const MediaDocumentOutline = "media:document-outline;record;textable"

// MediaDisboundPage is the media URN for disbound page - has list marker (array of page objects)
const MediaDisboundPage = "media:disbound-page;list;textable"

// MediaImageDescription is the media URN for vision inference output - textable, scalar by default
const MediaImageDescription = "media:image-description;textable"

// MediaTranscriptionOutput is the media URN for transcription output - has record marker
const MediaTranscriptionOutput = "media:record;textable;transcription"

// MediaDecision is the media URN for decision output (Make Decision) - scalar by default
const MediaDecision = "media:bool;decision;textable"

// MediaDecisionArray is the media URN for decision array output (Make Multiple Decisions) - has list marker
const MediaDecisionArray = "media:bool;decision;list;textable"
