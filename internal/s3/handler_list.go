package s3

import (
	"encoding/base64"
	"encoding/xml"
	"net/http"
	"net/url"
	"strconv"
)

type objectEntry struct {
	Key               string `xml:"Key"`
	LastModified      string `xml:"LastModified"`
	ETag              string `xml:"ETag"`
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
	Size              int64  `xml:"Size"`
	StorageClass      string `xml:"StorageClass"`
	Owner             *owner `xml:"Owner,omitempty"`
}

type listBucketResult struct {
	XMLName               xml.Name
	Name                  string         `xml:"Name"`
	Prefix                string         `xml:"Prefix"`
	Marker                *string        `xml:"Marker"`
	NextMarker            string         `xml:"NextMarker,omitempty"`
	ContinuationToken     string         `xml:"ContinuationToken,omitempty"`
	NextContinuationToken string         `xml:"NextContinuationToken,omitempty"`
	StartAfter            string         `xml:"StartAfter,omitempty"`
	KeyCount              *int           `xml:"KeyCount"`
	MaxKeys               int            `xml:"MaxKeys"`
	Delimiter             string         `xml:"Delimiter,omitempty"`
	EncodingType          string         `xml:"EncodingType,omitempty"`
	IsTruncated           bool           `xml:"IsTruncated"`
	Contents              []objectEntry  `xml:"Contents"`
	CommonPrefixes        []commonPrefix `xml:"CommonPrefixes"`
}

// listing holds what both ListObjects versions parse from the query.
type listing struct {
	params   listParams
	encoding string
	enc      func(string) string
}

func parseListing(q url.Values, marker string) (listing, error) {
	enc, err := keyEncoder(q.Get("encoding-type"))
	if err != nil {
		return listing{}, err
	}
	maxKeys := defaultMaxKeys
	if v := q.Get("max-keys"); v != "" {
		n, err := strconv.Atoi(v)
		if err != nil || n < 0 {
			return listing{}, errInvalidArgument("Provided max-keys not an integer or within integer range")
		}
		maxKeys = min(n, defaultMaxKeys)
	}
	return listing{
		params:   listParams{prefix: q.Get("prefix"), delimiter: q.Get("delimiter"), marker: marker, maxKeys: maxKeys},
		encoding: q.Get("encoding-type"),
		enc:      enc,
	}, nil
}

// listCurrent lists the current objects of a bucket and fills the shared
// fields of the result.
func (s *Service) listCurrent(name string, l listing, withOwner bool) (listBucketResult, listPage, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	b, err := s.bucketOf(name)
	if err != nil {
		return listBucketResult{}, listPage{}, err
	}
	var keys []string
	for _, k := range b.sortedKeys() {
		if b.current(k) != nil {
			keys = append(keys, k)
		}
	}
	page := listKeys(keys, l.params)
	out := listBucketResult{
		XMLName: xmlName("ListBucketResult"), Name: name, Prefix: l.enc(l.params.prefix), MaxKeys: l.params.maxKeys,
		Delimiter: l.enc(l.params.delimiter), EncodingType: l.encoding, IsTruncated: page.truncated,
		CommonPrefixes: commonPrefixes(page.prefixes, l.enc),
	}
	for _, k := range page.keys {
		out.Contents = append(out.Contents, s.entryOf(b.current(k), l.enc, withOwner))
	}
	return out, page, nil
}

func (s *Service) entryOf(o *object, enc func(string) string, withOwner bool) objectEntry {
	e := objectEntry{
		Key: enc(o.key), LastModified: xmlTime(o.lastModified), ETag: o.etag, Size: o.size(), StorageClass: "STANDARD",
		ChecksumAlgorithm: o.checksum.algorithm, ChecksumType: o.checksum.typ,
	}
	if withOwner {
		own := s.owner()
		e.Owner = &own
	}
	return e
}

// listObjectsV2 pages with an opaque continuation token: the base64 of the
// last key or common prefix returned.
func (s *Service) listObjectsV2(w http.ResponseWriter, name string, q url.Values) error {
	marker := q.Get("start-after")
	token := q.Get("continuation-token")
	if token != "" {
		raw, err := base64.RawURLEncoding.DecodeString(token)
		if err != nil {
			return errInvalidArgument("The continuation token provided is incorrect")
		}
		marker = string(raw)
	}
	l, err := parseListing(q, marker)
	if err != nil {
		return err
	}
	out, page, err := s.listCurrent(name, l, q.Get("fetch-owner") == "true")
	if err != nil {
		return err
	}
	count := len(page.keys) + len(page.prefixes)
	out.KeyCount = &count
	out.ContinuationToken = token
	out.StartAfter = l.enc(q.Get("start-after"))
	if page.truncated {
		out.NextContinuationToken = base64.RawURLEncoding.EncodeToString([]byte(page.nextMarker))
	}
	writeXML(w, out)
	return nil
}

func (s *Service) listObjectsV1(w http.ResponseWriter, name string, q url.Values) error {
	l, err := parseListing(q, q.Get("marker"))
	if err != nil {
		return err
	}
	out, page, err := s.listCurrent(name, l, true)
	if err != nil {
		return err
	}
	marker := l.enc(q.Get("marker"))
	out.Marker = &marker
	if page.truncated {
		out.NextMarker = l.enc(page.nextMarker)
	}
	writeXML(w, out)
	return nil
}

type listVersionsResult struct {
	XMLName             xml.Name
	Name                string         `xml:"Name"`
	Prefix              string         `xml:"Prefix"`
	KeyMarker           string         `xml:"KeyMarker"`
	VersionIDMarker     string         `xml:"VersionIdMarker"`
	NextKeyMarker       string         `xml:"NextKeyMarker,omitempty"`
	NextVersionIDMarker string         `xml:"NextVersionIdMarker,omitempty"`
	MaxKeys             int            `xml:"MaxKeys"`
	Delimiter           string         `xml:"Delimiter,omitempty"`
	EncodingType        string         `xml:"EncodingType,omitempty"`
	IsTruncated         bool           `xml:"IsTruncated"`
	Versions            []versionXML   `xml:"Version"`
	DeleteMarkers       []versionXML   `xml:"DeleteMarker"`
	CommonPrefixes      []commonPrefix `xml:"CommonPrefixes"`
}

type versionXML struct {
	Key               string `xml:"Key"`
	VersionID         string `xml:"VersionId"`
	IsLatest          bool   `xml:"IsLatest"`
	LastModified      string `xml:"LastModified"`
	ETag              string `xml:"ETag,omitempty"`
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
	Size              *int64 `xml:"Size"`
	StorageClass      string `xml:"StorageClass,omitempty"`
	Owner             owner  `xml:"Owner"`
}

func (s *Service) listObjectVersions(w http.ResponseWriter, name string, q url.Values) error {
	l, err := parseListing(q, q.Get("key-marker"))
	if err != nil {
		return err
	}
	versionMarker := q.Get("version-id-marker")
	s.mu.RLock()
	b, err := s.bucketOf(name)
	if err != nil {
		s.mu.RUnlock()
		return err
	}
	page := b.listVersions(l.params, versionMarker)
	s.mu.RUnlock()

	out := listVersionsResult{
		XMLName: xmlName("ListVersionsResult"), Name: name, Prefix: l.enc(l.params.prefix),
		KeyMarker: l.enc(l.params.marker), VersionIDMarker: versionMarker, MaxKeys: l.params.maxKeys,
		Delimiter: l.enc(l.params.delimiter), EncodingType: l.encoding, IsTruncated: page.truncated,
		CommonPrefixes: commonPrefixes(page.prefixes, l.enc),
	}
	if page.truncated {
		out.NextKeyMarker, out.NextVersionIDMarker = l.enc(page.nextKeyMarker), page.nextVersionMark
	}
	for _, v := range page.versions {
		x := versionXML{Key: l.enc(v.obj.key), VersionID: v.obj.wireVersionID(), IsLatest: v.isLatest,
			LastModified: xmlTime(v.obj.lastModified), Owner: s.owner()}
		if v.obj.deleteMarker {
			out.DeleteMarkers = append(out.DeleteMarkers, x)
			continue
		}
		size := v.obj.size()
		x.ETag, x.Size, x.StorageClass = v.obj.etag, &size, "STANDARD"
		x.ChecksumAlgorithm, x.ChecksumType = v.obj.checksum.algorithm, v.obj.checksum.typ
		out.Versions = append(out.Versions, x)
	}
	writeXML(w, out)
	return nil
}
