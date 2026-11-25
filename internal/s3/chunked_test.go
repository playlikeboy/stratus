package s3

import (
	"bytes"
	"strconv"
	"strings"
	"testing"
)

func TestDecodeAWSChunked(t *testing.T) {
	tests := []struct {
		name     string
		body     string
		want     string
		trailers map[string]string
	}{
		{
			name: "unsigned with trailer",
			body: "5\r\nhello\r\n6\r\n world\r\n0\r\nx-amz-checksum-crc32:DUoRhQ==\r\n\r\n",
			want: "hello world", trailers: map[string]string{"x-amz-checksum-crc32": "DUoRhQ=="},
		},
		{
			name: "signed chunks",
			body: "5;chunk-signature=" + strings.Repeat("a", 64) + "\r\nhello\r\n" +
				"0;chunk-signature=" + strings.Repeat("b", 64) + "\r\n\r\n",
			want: "hello", trailers: map[string]string{},
		},
		{
			name: "signed with trailer and trailer signature",
			body: "3;chunk-signature=x\r\nabc\r\n0;chunk-signature=y\r\n" +
				"x-amz-checksum-sha256:abc=\r\nx-amz-trailer-signature:zzz\r\n\r\n",
			want: "abc", trailers: map[string]string{"x-amz-checksum-sha256": "abc=", "x-amz-trailer-signature": "zzz"},
		},
		{
			name: "ends right after the final chunk",
			body: "2\r\nhi\r\n0\r\n",
			want: "hi", trailers: map[string]string{},
		},
		{
			name: "upper-case hex size",
			body: "A\r\n0123456789\r\n0\r\n\r\n",
			want: "0123456789", trailers: map[string]string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			for _, hint := range []int64{-1, int64(len(tt.want))} {
				data, trailers, err := decodeAWSChunked(strings.NewReader(tt.body), hint, 1<<20)
				if err != nil {
					t.Fatalf("hint %d: %v", hint, err)
				}
				if string(data) != tt.want {
					t.Fatalf("hint %d: data = %q, want %q", hint, data, tt.want)
				}
				if len(trailers) != len(tt.trailers) {
					t.Fatalf("trailers = %v, want %v", trailers, tt.trailers)
				}
				for k, v := range tt.trailers {
					if trailers[k] != v {
						t.Fatalf("trailer %s = %q, want %q", k, trailers[k], v)
					}
				}
			}
		})
	}
}

func TestDecodeAWSChunkedLarge(t *testing.T) {
	payload := bytes.Repeat([]byte("0123456789abcdef"), 40000) // 640000 bytes, several chunks
	var body bytes.Buffer
	for rest := payload; len(rest) > 0; {
		n := min(len(rest), 65536)
		body.WriteString(strconv.FormatInt(int64(n), 16) + "\r\n")
		body.Write(rest[:n])
		body.WriteString("\r\n")
		rest = rest[n:]
	}
	body.WriteString("0\r\n\r\n")
	data, _, err := decodeAWSChunked(&body, -1, 1<<30)
	if err != nil || !bytes.Equal(data, payload) {
		t.Fatalf("decode: err=%v equal=%v", err, bytes.Equal(data, payload))
	}
}

func TestDecodeAWSChunkedErrors(t *testing.T) {
	tests := map[string]string{
		"truncated chunk":    "10\r\nshort\r\n",
		"no final chunk":     "2\r\nhi\r\n",
		"bad size":           "zz\r\nhi\r\n0\r\n\r\n",
		"missing chunk CRLF": "2\r\nhiX0\r\n\r\n",
		"bad trailer":        "2\r\nhi\r\n0\r\nnocolon\r\n\r\n",
		"empty body":         "",
	}
	for name, body := range tests {
		t.Run(name, func(t *testing.T) {
			if _, _, err := decodeAWSChunked(strings.NewReader(body), -1, 1<<20); err == nil {
				t.Fatal("want an error")
			}
		})
	}
	if _, _, err := decodeAWSChunked(strings.NewReader("5\r\nhello\r\n0\r\n\r\n"), -1, 4); errorCode(err) != "EntityTooLarge" {
		t.Fatalf("over the limit: err = %v, want EntityTooLarge", err)
	}
}
