// Package capdag provides flat re-exports of all submodules
// This file should be created as capdag.go in the root after reorganization
// Matches Rust's src/lib.rs flat pub use re-exports
package capdag

// Re-export URN module
import (
	"github.com/machinefabric/capdag-go/bifaci"
	"github.com/machinefabric/capdag-go/cap"
	"github.com/machinefabric/capdag-go/media"
	"github.com/machinefabric/capdag-go/standard"
	"github.com/machinefabric/capdag-go/urn"
)

// URN types and functions
type CapUrn = urn.CapUrn
type MediaUrn = urn.MediaUrn
// CapMatrix is defined in cap_matrix.go - don't re-export

var NewCapUrnFromString = urn.NewCapUrnFromString
var NewCapUrnBuilder = urn.NewCapUrnBuilder
var NewMediaUrnFromString = urn.NewMediaUrnFromString

// Cap types and functions
type Cap = cap.Cap
type CapArg = cap.CapArg
type ArgSource = cap.ArgSource
type CapArgumentValue = cap.CapArgumentValue
type CapResult = cap.CapResult
type CapResultKind = cap.CapResultKind
type CapSet = cap.CapSet
type CapCaller = cap.CapCaller

// Media types
type MediaSpecDef = media.MediaSpecDef
type MediaUrnRegistry = media.MediaUrnRegistry
type ResolvedMediaSpec = media.ResolvedMediaSpec

// Bifaci (protocol) types - core frame types
type Frame = bifaci.Frame
type FrameType = bifaci.FrameType
type MessageId = bifaci.MessageId
type Limits = bifaci.Limits
type FrameReader = bifaci.FrameReader
type FrameWriter = bifaci.FrameWriter
type CartridgeRuntime = bifaci.CartridgeRuntime
type StreamEmitter = bifaci.StreamEmitter
type PeerInvoker = bifaci.PeerInvoker
type HandlerFunc = bifaci.HandlerFunc
type CapManifest = bifaci.CapManifest

var NewMessageIdFromUuid = bifaci.NewMessageIdFromUuid
var NewMessageIdFromUint = bifaci.NewMessageIdFromUint
var NewMessageIdRandom = bifaci.NewMessageIdRandom
var NewFrameReader = bifaci.NewFrameReader
var NewFrameWriter = bifaci.NewFrameWriter
var NewCartridgeRuntime = bifaci.NewCartridgeRuntime
var NewCapManifest = bifaci.NewCapManifest
var DecodeChunkPayload = bifaci.DecodeChunkPayload
var NewCapResultScalar = cap.NewCapResultScalar
var NewCapResultList = cap.NewCapResultList
var NewCapResultEmpty = cap.NewCapResultEmpty

// Standard caps (constants)
const CapIdentity = standard.CapIdentity
const CapDiscard = standard.CapDiscard

// Protocol constants
const ProtocolVersion = 2  // matches bifaci.PROTOCOL_VERSION
const DefaultMaxFrame = 16 * 1024 * 1024  // 16MB
const DefaultMaxChunk = 256 * 1024  // 256KB
