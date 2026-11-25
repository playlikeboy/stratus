package s3

import (
	"encoding/base64"
	"encoding/binary"
	"hash/crc64"
	"net/http"
	"testing"
)

// The published check values of each algorithm for "123456789".
func TestChecksumCheckValues(t *testing.T) {
	data := []byte("123456789")
	if got := crc64.Checksum(data, crc64NVMETable); got != 0xAE8B14860A799888 {
		t.Fatalf("CRC64NVME check value = %#x, want 0xae8b14860a799888", got)
	}
	tests := map[string][]byte{
		algoCRC32:     binary.BigEndian.AppendUint32(nil, 0xCBF43926),
		algoCRC32C:    binary.BigEndian.AppendUint32(nil, 0xE3069283),
		algoCRC64NVME: binary.BigEndian.AppendUint64(nil, 0xAE8B14860A799888),
	}
	for algo, raw := range tests {
		if got, want := computeChecksum(algo, data), base64.StdEncoding.EncodeToString(raw); got != want {
			t.Errorf("%s = %s, want %s", algo, got, want)
		}
	}
	if got := computeChecksum(algoSHA256, nil); got != "47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=" {
		t.Errorf("SHA256 of nothing = %s", got)
	}
	if got := computeChecksum(algoSHA1, nil); got != "2jmj7l5rSw0yVb/vlWAYkK/YBwk=" {
		t.Errorf("SHA1 of nothing = %s", got)
	}
}

func TestRequestedChecksumVerify(t *testing.T) {
	data := []byte("hello world")
	good := computeChecksum(algoCRC32, data)
	if got, err := (requestedChecksum{algorithm: algoCRC32, value: good}).verify(data); err != nil || got != good {
		t.Fatalf("matching checksum: %q, %v", got, err)
	}
	if _, err := (requestedChecksum{algorithm: algoCRC32, value: "AAAAAA=="}).verify(data); errorCode(err) != "BadDigest" {
		t.Fatalf("mismatch: err = %v, want BadDigest", err)
	}
	if got, err := (requestedChecksum{algorithm: algoSHA256}).verify(data); err != nil || got == "" {
		t.Fatalf("algorithm only: %q, %v", got, err)
	}
}

func TestChecksumFromHeaders(t *testing.T) {
	h := http.Header{}
	h.Set("x-amz-sdk-checksum-algorithm", "crc32c")
	got, err := checksumFromHeaders(h, nil)
	if err != nil || got.algorithm != algoCRC32C || got.value != "" {
		t.Fatalf("algorithm header: %+v, %v", got, err)
	}
	h.Set("x-amz-checksum-crc32c", "abc=")
	if got, _ := checksumFromHeaders(h, nil); got.value != "abc=" {
		t.Fatalf("value header: %+v", got)
	}
	got, err = checksumFromHeaders(h, map[string]string{"x-amz-checksum-sha1": "t=", "x-amz-trailer-signature": "s"})
	if err != nil || got.algorithm != algoSHA1 || got.value != "t=" {
		t.Fatalf("trailer: %+v, %v", got, err)
	}
	h = http.Header{}
	h.Set("x-amz-sdk-checksum-algorithm", "md4")
	if _, err := checksumFromHeaders(h, nil); err == nil {
		t.Fatal("unknown algorithm: want an error")
	}
}

func TestContentMD5(t *testing.T) {
	h := http.Header{}
	h.Set("Content-MD5", "XrY7u+Ae7tCTyyK7j1rNww==") // MD5 of "hello world"
	if err := verifyContentMD5(h, []byte("hello world")); err != nil {
		t.Fatalf("matching: %v", err)
	}
	if err := verifyContentMD5(h, []byte("hello")); errorCode(err) != "BadDigest" {
		t.Fatalf("mismatch: %v", err)
	}
	h.Set("Content-MD5", "nope")
	if err := verifyContentMD5(h, nil); errorCode(err) != "InvalidDigest" {
		t.Fatalf("malformed: %v", err)
	}
}

func TestMultipartETagAndComposite(t *testing.T) {
	a, b := etagOf([]byte("a")), etagOf([]byte("b"))
	if got := multipartETag([]string{a, b}); len(got) < 4 || got[len(got)-3:] != `-2"` {
		t.Fatalf("multipart ETag = %s", got)
	}
	sum, ok := compositeChecksum(algoCRC32, []string{computeChecksum(algoCRC32, []byte("a")), computeChecksum(algoCRC32, []byte("b"))})
	if !ok || sum[len(sum)-2:] != "-2" {
		t.Fatalf("composite = %q, %v", sum, ok)
	}
}

func TestNewUploadChecksum(t *testing.T) {
	tests := []struct {
		algo, typ, wantAlgo, wantType string
		fails                         bool
	}{
		{algo: "", typ: ""},
		{algo: "CRC64NVME", wantAlgo: algoCRC64NVME, wantType: checksumFullObject},
		{algo: "crc32", wantAlgo: algoCRC32, wantType: checksumComposite},
		{algo: "CRC32C", typ: "FULL_OBJECT", wantAlgo: algoCRC32C, wantType: checksumFullObject},
		{algo: "SHA256", typ: "FULL_OBJECT", fails: true},
		{algo: "CRC64NVME", typ: "COMPOSITE", fails: true},
		{algo: "", typ: "COMPOSITE", fails: true},
	}
	for _, tt := range tests {
		algo, typ, err := newUploadChecksum(tt.algo, tt.typ)
		if (err != nil) != tt.fails || algo != tt.wantAlgo || typ != tt.wantType {
			t.Errorf("%q/%q: got %q/%q err=%v", tt.algo, tt.typ, algo, typ, err)
		}
	}
}
