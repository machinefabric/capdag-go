// Cartridge registry slug — deterministic, human-readable mapping from a
// registry URL to a top-level folder name under the cartridges install root.
//
// Mirrors capdag::cartridge_slug byte-for-byte: the slug is a path-safe
// transform of the URL's AUTHORITY (host, plus ":port" if present) — the
// substring after "://" up to the next '/', '?', or '#' — lowercased, with
// every character outside [a-z0-9.-] replaced by '-' (so a port ':' becomes
// '-'). The manifest path (incl. the /v<N>/manifest version segment), query,
// and trailing slash are discarded, so the slug is version- and
// path-independent. No hashing — domains are unique and readable. The literal
// string "dev" is reserved for dev cartridges that have no registry.
//
// The mapping is one-way: folder → URL is recovered from each installed
// cartridge's own cartridge.json:registry_url. The host validates
// `SlugFor(cartridgeJson.RegistryURL) == folderName` at parse time.

package bifaci

import "strings"

// DevSlug is the reserved folder name for cartridges with no registry
// (developer-built cartridges installed via `dx cartridge --install` without
// `--registry`). A real registry authority is never the literal "dev".
const DevSlug = "dev"

// authorityOf returns the authority (host[:port]) of a registry URL: after
// "://" up to the next '/', '?', or '#' (path/query/fragment discarded).
func authorityOf(url string) string {
	afterScheme := url
	if i := strings.Index(url, "://"); i >= 0 {
		afterScheme = url[i+3:]
	}
	end := len(afterScheme)
	if j := strings.IndexAny(afterScheme, "/?#"); j >= 0 {
		end = j
	}
	return afterScheme[:end]
}

// slugFromAuthority applies the shared path-safe transform: ASCII-lowercase,
// with every char outside [a-z0-9.-] replaced by '-'. Shared by the cartridge
// slug and the fabric-cache slug so the two never diverge.
func slugFromAuthority(authority string) string {
	var b strings.Builder
	for _, r := range authority {
		if r >= 'A' && r <= 'Z' {
			r += 'a' - 'A'
		}
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-' {
			b.WriteRune(r)
		} else {
			b.WriteRune('-')
		}
	}
	return b.String()
}

// SlugFor computes the on-disk slug for a registry URL.
//
// `nil` (a dev cartridge) → the literal `DevSlug`. Non-nil → a path-safe
// transform of the URL's authority (see package doc). Depends ONLY on the
// authority — path (incl. the version segment), query, trailing slash, and host
// case do not change it.
func SlugFor(registryURL *string) string {
	if registryURL == nil {
		return DevSlug
	}
	return slugFromAuthority(authorityOf(*registryURL))
}

// IsRegistrySlug returns true if `s` could be a valid slug for a non-dev
// registry: a non-empty path-safe authority string ([a-z0-9.-]+) that is not
// the dev sentinel. Used by host scanners to distinguish dev folders from
// registry folders before they read any cartridge.json.
func IsRegistrySlug(s string) bool {
	if s == "" || s == DevSlug {
		return false
	}
	for _, r := range s {
		if !((r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '.' || r == '-') {
			return false
		}
	}
	return true
}
