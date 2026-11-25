package s3

import (
	"slices"
	"testing"
	"time"
)

var listingKeys = []string{
	"a.txt", "dir/a", "dir/b", "dir/sub/c", "dir/sub/d", "dir2/x", "z", "é", "日本",
}

func TestListKeysDelimiter(t *testing.T) {
	page := listKeys(listingKeys, listParams{delimiter: "/", maxKeys: 1000})
	if want := []string{"a.txt", "z", "é", "日本"}; !slices.Equal(page.keys, want) {
		t.Fatalf("keys = %v, want %v", page.keys, want)
	}
	if want := []string{"dir/", "dir2/"}; !slices.Equal(page.prefixes, want) {
		t.Fatalf("prefixes = %v, want %v", page.prefixes, want)
	}
	page = listKeys(listingKeys, listParams{prefix: "dir/", delimiter: "/", maxKeys: 1000})
	if !slices.Equal(page.keys, []string{"dir/a", "dir/b"}) || !slices.Equal(page.prefixes, []string{"dir/sub/"}) {
		t.Fatalf("under dir/: %+v", page)
	}
}

// Paging one entry at a time must visit each key or common prefix exactly
// once, in order, whatever the page size.
func TestListKeysPagination(t *testing.T) {
	want := []string{"a.txt", "dir/", "dir2/", "z", "é", "日本"}
	for size := 1; size <= len(want)+1; size++ {
		var got []string
		marker := ""
		for range 20 {
			page := listKeys(listingKeys, listParams{delimiter: "/", marker: marker, maxKeys: size})
			got = append(got, mergeSorted(page.keys, page.prefixes)...)
			if !page.truncated {
				break
			}
			marker = page.nextMarker
		}
		if !slices.Equal(got, want) {
			t.Fatalf("page size %d: got %v, want %v", size, got, want)
		}
	}
}

func mergeSorted(a, b []string) []string {
	out := slices.Concat(a, b)
	slices.Sort(out)
	return out
}

func TestListKeysEdges(t *testing.T) {
	if page := listKeys(listingKeys, listParams{maxKeys: 0}); page.truncated || len(page.keys) != 0 {
		t.Fatalf("maxKeys 0: %+v", page)
	}
	page := listKeys(listingKeys, listParams{marker: "dir/sub/c", maxKeys: 2})
	if !slices.Equal(page.keys, []string{"dir/sub/d", "dir2/x"}) || !page.truncated || page.nextMarker != "dir2/x" {
		t.Fatalf("start after: %+v", page)
	}
	if page := listKeys(listingKeys, listParams{prefix: "nothing", maxKeys: 10}); len(page.keys) != 0 {
		t.Fatalf("no match: %+v", page)
	}
}

func TestListVersionsPagination(t *testing.T) {
	b := newBucket("b", "r", time.Unix(0, 0))
	b.versioning = versioningEnabled
	for i, key := range []string{"k1", "k1", "k2", "k1", "k3"} {
		b.add(&object{key: key, versionID: string(rune('a' + i)), lastModified: time.Unix(int64(i), 0)})
	}
	b.deleteKey("k2", time.Unix(9, 0))

	var got []string
	keyMarker, versionMarker := "", ""
	for range 20 {
		page := b.listVersions(listParams{marker: keyMarker, maxKeys: 2}, versionMarker)
		for _, v := range page.versions {
			got = append(got, v.obj.key+"/"+v.obj.versionID+latestMark(v.isLatest))
		}
		if !page.truncated {
			break
		}
		keyMarker, versionMarker = page.nextKeyMarker, page.nextVersionMark
	}
	want := []string{"k1/d*", "k1/b", "k1/a", "k2/" + b.latest("k2").versionID + "*", "k2/c", "k3/e*"}
	if !slices.Equal(got, want) {
		t.Fatalf("versions = %v, want %v", got, want)
	}
}

func latestMark(latest bool) string {
	if latest {
		return "*"
	}
	return ""
}
