package s3

import (
	"encoding/xml"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
)

const maxPartsListed = 1000

type listPartsResult struct {
	XMLName              xml.Name
	Bucket               string    `xml:"Bucket"`
	Key                  string    `xml:"Key"`
	UploadID             string    `xml:"UploadId"`
	PartNumberMarker     int       `xml:"PartNumberMarker"`
	NextPartNumberMarker int       `xml:"NextPartNumberMarker"`
	MaxParts             int       `xml:"MaxParts"`
	IsTruncated          bool      `xml:"IsTruncated"`
	Parts                []partXML `xml:"Part"`
	Initiator            owner     `xml:"Initiator"`
	Owner                owner     `xml:"Owner"`
	StorageClass         string    `xml:"StorageClass"`
	ChecksumAlgorithm    string    `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType         string    `xml:"ChecksumType,omitempty"`
}

type partXML struct {
	PartNumber   int    `xml:"PartNumber"`
	LastModified string `xml:"LastModified"`
	ETag         string `xml:"ETag"`
	Size         int64  `xml:"Size"`
	checksumFields
}

// intParam reads a non-negative integer query parameter, capped at limit.
func intParam(q url.Values, name string, def, limit int) (int, error) {
	v := q.Get(name)
	if v == "" {
		return def, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 0 {
		return 0, errInvalidArgument("Provided " + name + " not an integer or within integer range")
	}
	return min(n, limit), nil
}

func (s *Service) listParts(w http.ResponseWriter, bucketName, key string, q url.Values) error {
	maxParts, err := intParam(q, "max-parts", maxPartsListed, maxPartsListed)
	if err != nil {
		return err
	}
	marker, err := intParam(q, "part-number-marker", 0, maxPartNum)
	if err != nil {
		return err
	}
	s.mu.RLock()
	_, u, err := s.uploadOf(bucketName, key, q.Get("uploadId"))
	var parts []*part
	if err == nil {
		parts = u.sortedParts()
	}
	s.mu.RUnlock()
	if err != nil {
		return err
	}
	out := listPartsResult{
		XMLName: xmlName("ListPartsResult"), Bucket: bucketName, Key: key, UploadID: u.id,
		PartNumberMarker: marker, MaxParts: maxParts, Initiator: s.owner(), Owner: s.owner(),
		StorageClass: "STANDARD", ChecksumAlgorithm: u.algorithm, ChecksumType: u.checksumType,
	}
	for _, p := range parts {
		if p.number <= marker {
			continue
		}
		if len(out.Parts) == maxParts {
			out.IsTruncated = true
			break
		}
		out.Parts = append(out.Parts, partXML{
			PartNumber: p.number, LastModified: xmlTime(p.lastModified), ETag: p.etag, Size: int64(len(p.data)),
			checksumFields: checksumXML(u.algorithm, p.checksum, ""),
		})
		out.NextPartNumberMarker = p.number
	}
	writeXML(w, out)
	return nil
}

type listMultipartUploadsResult struct {
	XMLName            xml.Name
	Bucket             string      `xml:"Bucket"`
	KeyMarker          string      `xml:"KeyMarker"`
	UploadIDMarker     string      `xml:"UploadIdMarker"`
	NextKeyMarker      string      `xml:"NextKeyMarker,omitempty"`
	NextUploadIDMarker string      `xml:"NextUploadIdMarker,omitempty"`
	Prefix             string      `xml:"Prefix"`
	MaxUploads         int         `xml:"MaxUploads"`
	IsTruncated        bool        `xml:"IsTruncated"`
	Uploads            []uploadXML `xml:"Upload"`
}

type uploadXML struct {
	Key               string `xml:"Key"`
	UploadID          string `xml:"UploadId"`
	Initiator         owner  `xml:"Initiator"`
	Owner             owner  `xml:"Owner"`
	StorageClass      string `xml:"StorageClass"`
	Initiated         string `xml:"Initiated"`
	ChecksumAlgorithm string `xml:"ChecksumAlgorithm,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
}

// listMultipartUploads lists the uploads in progress by key, then by
// initiation time. Delimiter grouping is not supported.
func (s *Service) listMultipartUploads(w http.ResponseWriter, bucketName string, q url.Values) error {
	if q.Get("delimiter") != "" {
		return errNotImplemented("ListMultipartUploads with a delimiter")
	}
	maxUploads, err := intParam(q, "max-uploads", defaultMaxKeys, defaultMaxKeys)
	if err != nil {
		return err
	}
	prefix, keyMarker, idMarker := q.Get("prefix"), q.Get("key-marker"), q.Get("upload-id-marker")
	uploads, err := withBucket(s, bucketName, func(b *bucket) []*upload {
		var out []*upload
		for _, u := range b.uploads {
			if strings.HasPrefix(u.key, prefix) {
				out = append(out, u)
			}
		}
		return out
	})
	if err != nil {
		return err
	}
	slices.SortFunc(uploads, func(a, b *upload) int {
		if c := strings.Compare(a.key, b.key); c != 0 {
			return c
		}
		return a.initiated.Compare(b.initiated)
	})
	out := listMultipartUploadsResult{
		XMLName: xmlName("ListMultipartUploadsResult"), Bucket: bucketName, KeyMarker: keyMarker,
		UploadIDMarker: idMarker, Prefix: prefix, MaxUploads: maxUploads,
	}
	// Uploads of the marker key are skipped up to and including idMarker, or
	// all of them when there is no idMarker.
	skipping := idMarker != ""
	for _, u := range uploads {
		if keyMarker != "" && (u.key < keyMarker || (u.key == keyMarker && idMarker == "")) {
			continue
		}
		if u.key == keyMarker && skipping {
			skipping = u.id != idMarker
			continue
		}
		if len(out.Uploads) == maxUploads {
			out.IsTruncated = true
			break
		}
		out.Uploads = append(out.Uploads, uploadXML{
			Key: u.key, UploadID: u.id, Initiator: s.owner(), Owner: s.owner(), StorageClass: "STANDARD",
			Initiated: xmlTime(u.initiated), ChecksumAlgorithm: u.algorithm, ChecksumType: u.checksumType,
		})
		out.NextKeyMarker, out.NextUploadIDMarker = u.key, u.id
	}
	writeXML(w, out)
	return nil
}
