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

// List types - URNs must match base.toml definitions

// MediaList is the media URN for untyped list - ordered sequence of opaque byte sequences
const MediaList = "media:list"

// MediaTextableList is the media URN for textable list - ordered sequence of textable values
const MediaTextableList = "media:list;textable"

// MediaStringList is the media URN for string list type - textable with list marker
const MediaStringList = "media:list;textable"

// MediaIntegerList is the media URN for integer list type - textable, numeric with list marker
const MediaIntegerList = "media:integer;list;textable;numeric"

// MediaNumberList is the media URN for number list type - textable, numeric with list marker
const MediaNumberList = "media:list;numeric;textable"

// MediaBooleanList is the media URN for boolean list type - uses "bool" with list marker
const MediaBooleanList = "media:bool;list;textable"

// MediaObjectList is the media URN for object list type - list of records (NOT textable)
// Use a specific format like JSON array for textable object lists.
const MediaObjectList = "media:list;record"

// Semantic media types for specialized content

// MediaPNG is the media URN for PNG image data
const MediaPNG = "media:image;png"

// MediaJPEG is the media URN for JPEG image data
const MediaJPEG = "media:jpeg;image"

// MediaGIF is the media URN for GIF image data
const MediaGIF = "media:gif;image"

// MediaBMP is the media URN for BMP image data
const MediaBMP = "media:bmp;image"

// MediaTIFF is the media URN for TIFF image data
const MediaTIFF = "media:tiff;image"

// MediaWEBP is the media URN for WebP image data
const MediaWEBP = "media:webp;image"

// MediaAudio is the media URN for audio data (wav, mp3, flac, etc.)
const MediaAudio = "media:wav;audio"

// MediaMP3 is the media URN for MP3 audio data
const MediaMP3 = "media:mp3;audio"

// MediaWAV is the media URN for WAV audio data
const MediaWAV = "media:wav;audio"

// MediaFLAC is the media URN for FLAC audio data
const MediaFLAC = "media:flac;audio"

// MediaOGG is the media URN for OGG audio data
const MediaOGG = "media:ogg;audio"

// MediaAAC is the media URN for AAC audio data
const MediaAAC = "media:aac;audio"

// MediaM4A is the media URN for M4A audio data
const MediaM4A = "media:m4a;audio"

// MediaAIFF is the media URN for AIFF audio data
const MediaAIFF = "media:aiff;audio"

// MediaOpus is the media URN for Opus audio data
const MediaOpus = "media:opus;audio"

// MediaVideo is the media URN for video data (mp4, webm, mov, etc.)
const MediaVideo = "media:video"

// MediaMP4 is the media URN for MP4 video data
const MediaMP4 = "media:mp4;video"

// MediaMOV is the media URN for MOV video data
const MediaMOV = "media:mov;video"

// MediaWEBM is the media URN for WebM video data
const MediaWEBM = "media:webm;video"

// MediaMKV is the media URN for MKV video data
const MediaMKV = "media:mkv;video"

// Semantic AI input types - distinguished by their purpose/context

// MediaAudioSpeech is the media URN for audio input containing speech for transcription (Whisper)
const MediaAudioSpeech = "media:audio;wav;speech"

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

// Format-specific variants for JSON, YAML, CSV

// MediaJSONValue is the media URN for a generic JSON value (scalar — string, number, boolean, null, or object)
const MediaJSONValue = "media:json;textable"

// MediaJSONRecord is the media URN for a JSON object (alias for MediaJSON)
const MediaJSONRecord = "media:json;record;textable"

// MediaJSONList is the media URN for a JSON array (list of values)
const MediaJSONList = "media:json;list;textable"

// MediaJSONListRecord is the media URN for a JSON array of objects (list of records)
const MediaJSONListRecord = "media:json;list;record;textable"

// MediaYAMLValue is the media URN for a generic YAML value (scalar — string, number, boolean, null, or mapping)
const MediaYAMLValue = "media:textable;yaml"

// MediaYAMLRecord is the media URN for a YAML mapping (alias for MediaYAML)
const MediaYAMLRecord = "media:record;textable;yaml"

// MediaYAMLList is the media URN for a YAML sequence (list of values)
const MediaYAMLList = "media:list;textable;yaml"

// MediaYAMLListRecord is the media URN for a YAML sequence of mappings (list of records)
const MediaYAMLListRecord = "media:list;record;textable;yaml"

// MediaCSV is the media URN for CSV data — by definition a list of records (header row + data rows)
const MediaCSV = "media:csv;list;record;textable"

// MediaCSVList is the media URN for single-column CSV — list of values without record structure
const MediaCSVList = "media:csv;list;record;textable"

// File path type — for arguments that represent filesystem paths.
// There is a single media URN; cardinality (single file vs many files)
// is carried on the wire via is_sequence, not via URN tags.
const MediaFilePath = "media:file-path;textable"

// Semantic text input types - distinguished by their purpose/context

// MediaTextablePage is the media URN for extracted page text
const MediaTextablePage = "media:textable;page"

// MediaModelSpec is the media URN for model spec (provider:model format, HuggingFace name, etc.) - scalar by default
// Generic, backend-agnostic — used by inference caps for download/status/path operations.
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

// MediaImageDescription is the media URN for vision inference output - textable, scalar by default
const MediaImageDescription = "media:image-description;textable"

// MediaTranscriptionOutput is the media URN for transcription output - has record marker
const MediaTranscriptionOutput = "media:record;textable;transcription"

// MediaDecision is the media URN for decision output (Make Decision) - scalar by default
const MediaDecision = "media:decision;json;record;textable"

// MediaHFToken is the media URN for a Hugging Face API token (secret, textable)
const MediaHFToken = "media:hf-token;secret;textable"

// MediaModelArchList is the media URN for a list of model architectures — JSON record
const MediaModelArchList = "media:model-arch-list;json;record;textable"

// MediaModelSearchRequest is the media URN for a model search request — JSON record
const MediaModelSearchRequest = "media:model-search-request;json;record;textable"

// MediaModelSearchResponse is the media URN for a model search response — JSON record
const MediaModelSearchResponse = "media:model-search-response;json;record;textable"

// MediaModelFilterResolution is the media URN for model filter resolution — JSON record
const MediaModelFilterResolution = "media:model-filter-resolution;json;record;textable"

// MediaCollection is the media URN for a collection (map/record form)
const MediaCollection = "media:collection;record;textable"

// MediaAdapterSelection is the media URN for adapter selection output - JSON record
const MediaAdapterSelection = "media:adapter-selection;json;record"

// MediaCapURN is the media URN for a canonical cap URN string carried as data.
// Consumed by cap:lookup-cap;fabric.
const MediaCapURN = "media:cap-urn;textable"

// MediaMediaURN is the media URN for a canonical media URN string carried as data.
// Consumed by cap:lookup-media-spec;fabric.
const MediaMediaURN = "media:media-urn;textable"

// MediaCapDefinition is the media URN for the full flattened cap definition
// produced by cap:lookup-cap;fabric.
const MediaCapDefinition = "media:cap-definition;json;record;textable"

// MediaMediaSpecDefinition is the media URN for the full media spec definition
// produced by cap:lookup-media-spec;fabric.
const MediaMediaSpecDefinition = "media:media-spec-definition;json;record;textable"

// MediaCollectionList is the media URN for a list of collections
const MediaCollectionList = "media:collection;list;record;textable"
