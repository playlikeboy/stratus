package s3

import (
	"crypto/md5"
	"crypto/sha1"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"hash"
	"hash/crc32"
	"hash/crc64"
	"net/http"
	"strconv"
	"strings"
)

// The checksum algorithms S3 accepts, in the spelling of
// x-amz-sdk-checksum-algorithm.
const (
	algoCRC32     = "CRC32"
	algoCRC32C    = "CRC32C"
	algoCRC64NVME = "CRC64NVME"
	algoSHA1      = "SHA1"
	algoSHA256    = "SHA256"
)

// Checksum types: a full-object checksum covers every byte of the object; a
// composite one is the checksum of the parts' checksums of a multipart upload.
const (
	checksumFullObject = "FULL_OBJECT"
	checksumComposite  = "COMPOSITE"
)

// algorithms is the order a response lists checksums in, and the order a
// request's headers are searched in.
var algorithms = []string{algoCRC32, algoCRC32C, algoCRC64NVME, algoSHA1, algoSHA256}

// crc64NVMETable is the table of the CRC-64/NVME polynomial
// (0xAD93D23594C93659), given to crc64 bit-reversed as it expects. crc64.Update
// already applies the initial and final inversion that CRC-64/NVME specifies.
var crc64NVMETable = crc64.MakeTable(0x9A6C9329AC4BC9B5)

var crc32cTable = crc32.MakeTable(crc32.Castagnoli)

// checksum is a stored checksum of an object or a part.
type checksum struct {
	algorithm string // empty: none
	value     string // base64; "<base64>-<n>" for a composite multipart checksum
	typ       string // checksumFullObject or checksumComposite
}

// newHash returns a hash for a checksum algorithm, or nil when it is unknown.
func newHash(algorithm string) hash.Hash {
	switch algorithm {
	case algoCRC32:
		return crc32.NewIEEE()
	case algoCRC32C:
		return crc32.New(crc32cTable)
	case algoCRC64NVME:
		return crc64.New(crc64NVMETable)
	case algoSHA1:
		return sha1.New()
	case algoSHA256:
		return sha256.New()
	}
	return nil
}

// isCRC tells whether an algorithm can be computed over a whole multipart
// object (FULL_OBJECT); the SHA algorithms only support composite checksums.
func isCRC(algorithm string) bool {
	return algorithm == algoCRC32 || algorithm == algoCRC32C || algorithm == algoCRC64NVME
}

// checksumHeader is the header that carries a checksum of an algorithm.
func checksumHeader(algorithm string) string {
	return "x-amz-checksum-" + strings.ToLower(algorithm)
}

// normalizeAlgorithm upper-cases an algorithm name and tells whether it is known.
func normalizeAlgorithm(name string) (string, bool) {
	upper := strings.ToUpper(strings.TrimSpace(name))
	return upper, newHash(upper) != nil
}

// computeChecksum returns the base64 checksum of data.
func computeChecksum(algorithm string, data []byte) string {
	h := newHash(algorithm)
	_, _ = h.Write(data) // hash writes never fail
	return base64.StdEncoding.EncodeToString(h.Sum(nil))
}

// defaultChecksum is the checksum S3 gives an object uploaded without one: a
// full-object CRC64NVME.
func defaultChecksum(data []byte) checksum {
	return checksum{algorithm: algoCRC64NVME, value: computeChecksum(algoCRC64NVME, data), typ: checksumFullObject}
}

// compositeChecksum is the checksum of the concatenated raw part checksums,
// suffixed with the number of parts, as S3 reports for a COMPOSITE upload.
func compositeChecksum(algorithm string, partChecksums []string) (string, bool) {
	h := newHash(algorithm)
	for _, c := range partChecksums {
		raw, err := base64.StdEncoding.DecodeString(c)
		if err != nil {
			return "", false
		}
		_, _ = h.Write(raw)
	}
	return base64.StdEncoding.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(partChecksums)), true
}

// etagOf is the quoted hex MD5 of data, the ETag of a single-part object.
func etagOf(data []byte) string {
	sum := md5.Sum(data)
	return `"` + hex.EncodeToString(sum[:]) + `"`
}

// multipartETag is the quoted MD5 of the parts' binary MD5s, suffixed with
// the number of parts.
func multipartETag(partETags []string) string {
	h := md5.New()
	for _, e := range partETags {
		raw, _ := hex.DecodeString(strings.Trim(e, `"`)) // part ETags are made here, always hex
		_, _ = h.Write(raw)
	}
	return `"` + hex.EncodeToString(h.Sum(nil)) + "-" + strconv.Itoa(len(partETags)) + `"`
}

// requestedChecksum is the checksum a request announces: the algorithm and,
// when the client sent it, the expected value.
type requestedChecksum struct {
	algorithm string
	value     string // empty when only the algorithm was named
}

// checksumFromHeaders reads the checksum a request carries in its headers or
// its aws-chunked trailers. A trailer named by x-amz-trailer wins.
func checksumFromHeaders(h http.Header, trailers map[string]string) (requestedChecksum, error) {
	for name, value := range trailers {
		if algo, ok := strings.CutPrefix(name, "x-amz-checksum-"); ok {
			upper, known := normalizeAlgorithm(algo)
			if !known {
				return requestedChecksum{}, errInvalidRequest("Unsupported checksum trailer " + name + ".")
			}
			return requestedChecksum{algorithm: upper, value: value}, nil
		}
	}
	for _, algo := range algorithms {
		if v := h.Get(checksumHeader(algo)); v != "" {
			return requestedChecksum{algorithm: algo, value: v}, nil
		}
	}
	if name := h.Get("x-amz-sdk-checksum-algorithm"); name != "" {
		upper, known := normalizeAlgorithm(name)
		if !known {
			return requestedChecksum{}, errInvalidRequest("Checksum algorithm " + name + " is not supported.")
		}
		return requestedChecksum{algorithm: upper}, nil
	}
	return requestedChecksum{}, nil
}

// verify computes the checksum of data and compares it with the expected
// value, if any; it returns the computed value.
func (c requestedChecksum) verify(data []byte) (string, error) {
	if c.algorithm == "" {
		return "", nil
	}
	got := computeChecksum(c.algorithm, data)
	if c.value != "" && c.value != got {
		return "", errBadDigest("The " + c.algorithm + " you specified did not match the calculated checksum.")
	}
	return got, nil
}

// verifyContentMD5 checks a Content-MD5 header against data.
func verifyContentMD5(h http.Header, data []byte) error {
	v := h.Get("Content-Md5")
	if v == "" {
		return nil
	}
	want, err := base64.StdEncoding.DecodeString(v)
	if err != nil || len(want) != md5.Size {
		return errInvalidDigest()
	}
	if sum := md5.Sum(data); string(sum[:]) != string(want) {
		return errBadDigest("The Content-MD5 you specified did not match what we received.")
	}
	return nil
}
