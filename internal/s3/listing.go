package s3

import (
	"slices"
	"sort"
	"strings"
)

// defaultMaxKeys is both the default and the largest page of a listing.
const defaultMaxKeys = 1000

// listParams are the inputs the listing operations share. marker is the
// exclusive start: a key, or a common prefix returned by the previous page.
type listParams struct {
	prefix    string
	delimiter string
	marker    string
	maxKeys   int
}

// listPage is one page of keys and common prefixes, both in UTF-8 byte order.
type listPage struct {
	keys       []string
	prefixes   []string
	truncated  bool
	nextMarker string // the last key or common prefix of the page
}

// rollup returns the common prefix key falls under, if the delimiter applies.
func (p listParams) rollup(key string) (string, bool) {
	if p.delimiter == "" {
		return "", false
	}
	i := strings.Index(key[len(p.prefix):], p.delimiter)
	if i < 0 {
		return "", false
	}
	return key[:len(p.prefix)+i+len(p.delimiter)], true
}

// startIndex is where the scan of sorted keys begins: at the prefix, or
// just after the marker, whichever is later.
func (p listParams) startIndex(sorted []string) int {
	return sort.SearchStrings(sorted, max(p.prefix, p.marker))
}

// listKeys pages through sorted keys. A common prefix counts as one entry
// against maxKeys; a page that ends on a common prefix resumes after every
// key under it, because such keys roll up into the prefix already returned.
func listKeys(sorted []string, p listParams) listPage {
	var page listPage
	if p.maxKeys <= 0 {
		return page
	}
	lastPrefix := ""
	for _, key := range sorted[p.startIndex(sorted):] {
		if key <= p.marker {
			continue
		}
		if !strings.HasPrefix(key, p.prefix) {
			break // sorted: no later key has the prefix either
		}
		entry, isPrefix := p.rollup(key)
		if isPrefix && (entry == lastPrefix || entry == p.marker) {
			continue
		}
		if len(page.keys)+len(page.prefixes) == p.maxKeys {
			page.truncated = true
			break
		}
		if isPrefix {
			page.prefixes = append(page.prefixes, entry)
			lastPrefix = entry
			page.nextMarker = entry
			continue
		}
		page.keys = append(page.keys, key)
		page.nextMarker = key
	}
	return page
}

// versionEntry is one version in a ListObjectVersions page.
type versionEntry struct {
	obj      *object
	isLatest bool
}

// versionPage is one page of ListObjectVersions.
type versionPage struct {
	versions        []versionEntry
	prefixes        []string
	truncated       bool
	nextKeyMarker   string
	nextVersionMark string
}

// listVersions pages through every version, keys in byte order and the
// versions of a key newest first. The caller holds s.mu.
func (b *bucket) listVersions(p listParams, versionMarker string) versionPage {
	var page versionPage
	if p.maxKeys <= 0 {
		return page
	}
	sorted := b.sortedKeys()
	lastPrefix := ""
	for _, key := range sorted[sort.SearchStrings(sorted, max(p.prefix, p.marker)):] {
		if !strings.HasPrefix(key, p.prefix) {
			break
		}
		if key == p.marker && versionMarker == "" {
			continue
		}
		if entry, isPrefix := p.rollup(key); isPrefix {
			if entry == lastPrefix || entry == p.marker {
				continue
			}
			if page.full(p.maxKeys) {
				return page
			}
			page.prefixes = append(page.prefixes, entry)
			lastPrefix = entry
			page.nextKeyMarker, page.nextVersionMark = entry, ""
			continue
		}
		if !page.addKey(b.objects[key], key == p.marker, versionMarker, p.maxKeys) {
			return page
		}
	}
	return page
}

// full marks the page truncated when it has no room for another entry.
func (page *versionPage) full(maxKeys int) bool {
	if len(page.versions)+len(page.prefixes) >= maxKeys {
		page.truncated = true
	}
	return page.truncated
}

// addKey appends the versions of one key, newest first, starting after
// versionMarker when the key is the marker key. It returns false once the
// page is full.
func (page *versionPage) addKey(versions []*object, isMarkerKey bool, versionMarker string, maxKeys int) bool {
	newestFirst := slices.Clone(versions)
	slices.Reverse(newestFirst)
	if isMarkerKey {
		i := slices.IndexFunc(newestFirst, func(o *object) bool { return o.wireVersionID() == versionMarker })
		newestFirst = newestFirst[i+1:] // an unknown marker (i = -1) starts at the newest
	}
	for _, o := range newestFirst {
		if page.full(maxKeys) {
			return false
		}
		page.versions = append(page.versions, versionEntry{obj: o, isLatest: o == versions[len(versions)-1]})
		page.nextKeyMarker, page.nextVersionMark = o.key, o.wireVersionID()
	}
	return true
}
