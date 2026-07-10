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
// - `media:ext=pdf` → scalar, opaque (no markers)
// - `media:enc=utf-8;list` → list, opaque (has list marker)
// - `media:fmt=json;record` → scalar, record (has record marker)
// - `media:fmt=json;list;record` → list of records (has both markers)

// Primitive types - URNs must match base.toml definitions

// MediaVoid is the media URN for void (no input/output) - no coercion tags
const MediaVoid = "media:void"

// MediaString is the media URN for string type — bare UTF-8 text (enc=utf-8), scalar by default (no list marker)
const MediaString = "media:enc=utf-8"

// MediaInteger is the media URN for integer type — numeric (math ops valid), scalar by default
const MediaInteger = "media:integer;numeric"

// MediaNumber is the media URN for number type — numeric, scalar by default
const MediaNumber = "media:numeric"

// MediaBoolean is the media URN for boolean type - uses "bool" not "boolean" per base.toml
const MediaBoolean = "media:bool;enc=utf-8"

// MediaObject is the media URN for a generic record/object type - has internal key-value structure,
// no content-format claim. Use MediaJSON for JSON-serialized objects.
const MediaObject = "media:record"

// MediaIdentity is the media URN for the top type - the most general media type (no constraints)
const MediaIdentity = "media:"

// List types - URNs must match base.toml definitions

// MediaList is the media URN for untyped list - ordered sequence of opaque byte sequences
const MediaList = "media:list"

// MediaStringList is the media URN for string list type — ordered sequence of bare UTF-8 text values
const MediaStringList = "media:enc=utf-8;list"

// MediaIntegerList is the media URN for integer list type — numeric with list marker
const MediaIntegerList = "media:integer;list;numeric"

// MediaNumberList is the media URN for number list type — numeric with list marker
const MediaNumberList = "media:list;numeric"

// MediaBooleanList is the media URN for boolean list type - uses "bool" with list marker
const MediaBooleanList = "media:bool;enc=utf-8;list"

// MediaObjectList is the media URN for object list type - list of records (no content-format claim)
// Use a specific format like JSON array for serialized object lists.
const MediaObjectList = "media:list;record"

// Semantic media types for specialized content

// MediaPNG is the media URN for PNG image data
const MediaPNG = "media:ext=png;image"

// MediaJPEG is the media URN for JPEG image data
const MediaJPEG = "media:ext=jpeg;image"

// MediaGIF is the media URN for GIF image data
const MediaGIF = "media:ext=gif;image"

// MediaBMP is the media URN for BMP image data
const MediaBMP = "media:ext=bmp;image"

// MediaTIFF is the media URN for TIFF image data
const MediaTIFF = "media:ext=tiff;image"

// MediaWEBP is the media URN for WebP image data
const MediaWEBP = "media:ext=webp;image"

// MediaAudio is the media URN for audio data (wav, mp3, flac, etc.)
const MediaAudio = "media:audio;ext=wav"

// MediaMP3 is the media URN for MP3 audio data
const MediaMP3 = "media:audio;ext=mp3"

// MediaWAV is the media URN for WAV audio data
const MediaWAV = "media:audio;ext=wav"

// MediaFLAC is the media URN for FLAC audio data
const MediaFLAC = "media:audio;ext=flac"

// MediaOGG is the media URN for OGG audio data
const MediaOGG = "media:audio;ext=ogg"

// MediaAAC is the media URN for AAC audio data
const MediaAAC = "media:audio;ext=aac"

// MediaM4A is the media URN for M4A audio data
const MediaM4A = "media:audio;ext=m4a"

// MediaAIFF is the media URN for AIFF audio data
const MediaAIFF = "media:audio;ext=aiff"

// MediaOpus is the media URN for Opus audio data
const MediaOpus = "media:audio;ext=opus"

// MediaVideo is the media URN for video data (mp4, webm, mov, etc.)
const MediaVideo = "media:video"

// MediaMP4 is the media URN for MP4 video data
const MediaMP4 = "media:ext=mp4;video"

// MediaMOV is the media URN for MOV video data
const MediaMOV = "media:ext=mov;video"

// MediaWEBM is the media URN for WebM video data
const MediaWEBM = "media:ext=webm;video"

// MediaMKV is the media URN for MKV video data
const MediaMKV = "media:ext=mkv;video"

// Semantic AI input types - distinguished by their purpose/context

// MediaAudioSpeech is the media URN for audio input containing speech for transcription (Whisper)
const MediaAudioSpeech = "media:audio;ext=wav;speech"

// Document types (PRIMARY naming - type IS the format)

// MediaPDF is the media URN for PDF documents
const MediaPDF = "media:ext=pdf"

// MediaEPUB is the media URN for EPUB documents
const MediaEPUB = "media:ext=epub"

// Text format types (PRIMARY naming - type IS the format)

// MediaMarkdown is the media URN for Markdown text
const MediaMarkdown = "media:enc=utf-8;ext=md"

// MediaTXT is the media URN for plain text
const MediaTXT = "media:enc=utf-8;ext=txt"

// MediaRST is the media URN for reStructuredText
const MediaRST = "media:enc=utf-8;ext=rst"

// MediaLog is the media URN for log files
const MediaLog = "media:enc=utf-8;ext=log"

// MediaHTML is the media URN for HTML documents
const MediaHTML = "media:enc=utf-8;ext=html"

// MediaXML is the media URN for XML documents
const MediaXML = "media:enc=utf-8;ext=xml"

// MediaJSON is the media URN for JSON data - has record marker (structured key-value)
const MediaJSON = "media:fmt=json;record"

// MediaJSONSchema is the media URN for JSON with schema constraint (input for structured queries)
const MediaJSONSchema = "media:fmt=json;json-schema;record"

// MediaYAML is the media URN for YAML data - has record marker (structured key-value)
const MediaYAML = "media:fmt=yaml;record"

// Format-specific variants for JSON, YAML, CSV

// MediaJSONValue is the media URN for a generic JSON value (scalar — string, number, boolean, null, or object)
const MediaJSONValue = "media:fmt=json"

// MediaJSONRecord is the media URN for a JSON object (alias for MediaJSON)
const MediaJSONRecord = "media:fmt=json;record"

// MediaJSONList is the media URN for a JSON array (list of values)
const MediaJSONList = "media:fmt=json;list"

// MediaJSONListRecord is the media URN for a JSON array of objects (list of records)
const MediaJSONListRecord = "media:fmt=json;list;record"

// MediaYAMLValue is the media URN for a generic YAML value (scalar — string, number, boolean, null, or mapping)
const MediaYAMLValue = "media:fmt=yaml"

// MediaYAMLRecord is the media URN for a YAML mapping (alias for MediaYAML)
const MediaYAMLRecord = "media:fmt=yaml;record"

// MediaYAMLList is the media URN for a YAML sequence (list of values)
const MediaYAMLList = "media:fmt=yaml;list"

// MediaYAMLListRecord is the media URN for a YAML sequence of mappings (list of records)
const MediaYAMLListRecord = "media:fmt=yaml;list;record"

// MediaCSV is the media URN for CSV data — by definition a list of records (header row + data rows)
const MediaCSV = "media:fmt=csv;list;record"

// MediaCSVList is the media URN for single-column CSV — list of values without record structure
const MediaCSVList = "media:fmt=csv;list;record"

// File path type — for arguments that represent filesystem paths.
// There is a single media URN; cardinality (single file vs many files)
// is carried on the wire via is_sequence, not via URN tags.
const MediaFilePath = "media:enc=utf-8;file-path"

// Semantic text input types - distinguished by their purpose/context

// MediaTextablePage is the media URN for a single page of finalised plain
// text extracted from a multi-page document (e.g. cap:disbind-pdf, one item
// per page). Carries `role=page`, `plain-text` (the opt-in marker for
// cap:save-as-txt's persistence path), and `file-type=txt`.
const MediaTextablePage = "media:enc=utf-8;ext=txt;page;plain-text"

// MediaModelSpec is the media URN for model spec (provider:model format, HuggingFace name, etc.) - scalar by default
// Generic, backend-agnostic — used by inference caps for download/status/path operations.
const MediaModelSpec = "media:enc=utf-8;model-spec"

// Backend + use-case specific model-spec variants.
// Each inference cap declares the variant matching its backend and purpose,
// so slot values can target a specific cartridge+task without ambiguity.

// GGUF backend

// MediaModelSpecGGUFVision is the GGUF vision model spec (e.g. moondream2)
const MediaModelSpecGGUFVision = "media:enc=utf-8;gguf;model-spec;tokenizer-embedded-gguf;vision"

// MediaModelSpecGGUFLLM is the GGUF LLM model spec (e.g. Mistral-7B)
const MediaModelSpecGGUFLLM = "media:enc=utf-8;gguf;llm;model-spec;tokenizer-embedded-gguf"

// MediaModelSpecGGUFEmbeddings is the GGUF embeddings model spec (e.g. nomic-embed)
const MediaModelSpecGGUFEmbeddings = "media:embeddings;enc=utf-8;gguf;model-spec;tokenizer-embedded-gguf"

// MLX backend

// MediaModelSpecMLXVision is the MLX vision model spec (e.g. Qwen3-VL)
const MediaModelSpecMLXVision = "media:enc=utf-8;mlx;model-spec;vision"

// MediaModelSpecMLXLLM is the MLX LLM model spec (e.g. Llama-3.2-3B)
const MediaModelSpecMLXLLM = "media:enc=utf-8;llm;mlx;model-spec"

// MediaModelSpecMLXEmbeddings is the MLX embeddings model spec (e.g. all-MiniLM-L6-v2)
const MediaModelSpecMLXEmbeddings = "media:embeddings;enc=utf-8;mlx;model-spec"

// Candle backend

// MediaModelSpecCandleVision is the Candle vision model spec (e.g. BLIP)
const MediaModelSpecCandleVision = "media:candle;enc=utf-8;model-spec;repo-safetensors;tokenizer-unified;vision"

// MediaModelSpecCandleEmbeddings is the Candle text embeddings model spec (e.g. BERT)
const MediaModelSpecCandleEmbeddings = "media:candle;embeddings;enc=utf-8;model-spec;repo-safetensors;tokenizer-unified"

// MediaModelSpecCandleImageEmbeddings is the Candle image embeddings model spec (e.g. CLIP)
const MediaModelSpecCandleImageEmbeddings = "media:candle;enc=utf-8;image-embeddings;model-spec"

// MediaModelSpecCandleTranscription is the Candle transcription model spec (e.g. Whisper)
const MediaModelSpecCandleTranscription = "media:candle;enc=utf-8;model-spec;repo-safetensors;tokenizer-unified;transcription"

// MediaMLXModelPath is the media URN for MLX model path - scalar by default
const MediaMLXModelPath = "media:enc=utf-8;mlx-model-path"

// MediaModelRepo is the media URN for model repository (input for list-models) - has record marker
const MediaModelRepo = "media:enc=utf-8;model-repo;record"

// CAPDAG output types - record marker for structured JSON objects, list marker for arrays

// MediaModelDim is the media URN for model dimension output - scalar by default (no list marker)
const MediaModelDim = "media:integer;model-dim;numeric"

// MediaDownloadOutput is the media URN for model download output - has record marker
const MediaDownloadOutput = "media:download-result;enc=utf-8;record"

// MediaListOutput is the media URN for model list output - has record marker
const MediaListOutput = "media:enc=utf-8;model-list;record"

// MediaStatusOutput is the media URN for model status output - has record marker
const MediaStatusOutput = "media:enc=utf-8;model-status;record"

// MediaContentsOutput is the media URN for model contents output - has record marker
const MediaContentsOutput = "media:enc=utf-8;model-contents;record"

// MediaAvailabilityOutput is the media URN for model availability output - has record marker
const MediaAvailabilityOutput = "media:enc=utf-8;model-availability;record"

// MediaPathOutput is the media URN for model path output - has record marker
const MediaPathOutput = "media:enc=utf-8;model-path;record"

// MediaEmbeddingVector is the media URN for embedding vector output - has record marker
const MediaEmbeddingVector = "media:embedding-vector;enc=utf-8;record"

// MediaImageDescription is the media URN for vision inference output — a
// concrete plain-text terminal. Carries `image-description` (the vision-specific
// marker), `plain-text` (the finalised-text marker that opts into
// cap:save-as-txt's persistence path), and `file-type=txt` (binds the URN to
// the `.txt` extension at the registry).
const MediaImageDescription = "media:enc=utf-8;ext=txt;image-description;plain-text"

// MediaPlainText is the media URN for finalised plain text — the canonical
// input/output of cap:save-as-txt. Producers of user-facing prose
// (LLM text-generation, OCR's extracted text, summarisation) declare this
// URN as their `out` so the planner restricts the .txt persistence path
// to those caps. See fabric/media/plain-text.toml.
const MediaPlainText = "media:enc=utf-8;ext=txt;plain-text"

// MediaTranscriptionOutput is the media URN for transcription output - has record marker
const MediaTranscriptionOutput = "media:enc=utf-8;record;transcription"

// MediaDecision is the media URN for decision output (Make Decision) - scalar by default
const MediaDecision = "media:decision;fmt=json;record"

// MediaSemanticJudgment is the media URN for a semantic-judgment record — the
// shared output envelope of the semantic-primitive cap family (`same`, and
// the primitives that follow it): `{result..., confidence, reason}`.
// See docs/semantic-primitives.md (law P2).
const MediaSemanticJudgment = "media:fmt=json;record;semantic-judgment"

// MediaHFToken is the media URN for a Hugging Face API token (secret)
const MediaHFToken = "media:enc=utf-8;hf-token;secret"

// MediaModelArchList is the media URN for a list of model architectures — JSON record
const MediaModelArchList = "media:fmt=json;model-arch-list;record"

// MediaModelSearchRequest is the media URN for a model search request — JSON record
const MediaModelSearchRequest = "media:fmt=json;model-search-request;record"

// MediaModelSearchResponse is the media URN for a model search response — JSON record
const MediaModelSearchResponse = "media:fmt=json;model-search-response;record"

// MediaModelFilterResolution is the media URN for model filter resolution — JSON record
const MediaModelFilterResolution = "media:fmt=json;model-filter-resolution;record"

// MediaCollection is the media URN for a collection (map/record form)
const MediaCollection = "media:collection;enc=utf-8;record"

// MediaAdapterSelection is the media URN for adapter selection output - JSON record
const MediaAdapterSelection = "media:adapter-selection;fmt=json;record"

// MediaCapURN is the media URN for a canonical cap URN string carried as data.
// Consumed by cap:lookup-cap;fabric.
const MediaCapURN = "media:cap-urn;enc=utf-8"

// MediaMediaURN is the media URN for a canonical media URN string carried as data.
// Consumed by cap:lookup-media-def;fabric.
const MediaMediaURN = "media:enc=utf-8;media-urn"

// MediaCapDefinition is the media URN for the full flattened cap definition
// produced by cap:lookup-cap;fabric.
const MediaCapDefinition = "media:cap-definition;fmt=json;record"

// MediaMediaDefinition is the media URN for the full media definition
// produced by cap:lookup-media-def;fabric.
const MediaMediaDefinition = "media:fmt=json;media-definition;record"

// MediaFabricDefver is the media URN for a fabric registry per-definition version (defver).
// Carried as data alongside a URN when a cap looks up a definition pinned to a specific
// manifest snapshot. Absent ⇒ defver 0 (legacy v0 flat-path lookup).
const MediaFabricDefver = "media:defver;enc=utf-8"

// MediaCollectionList is the media URN for a list of collections
const MediaCollectionList = "media:collection;enc=utf-8;list;record"
