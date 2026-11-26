package s3

import (
	"errors"
	"testing"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

func TestParseRange(t *testing.T) {
	const size = 100
	tests := []struct {
		header  string
		want    byteRange
		partial bool
		invalid bool
	}{
		{header: "", partial: false},
		{header: "bytes=0-9", want: byteRange{0, 9}, partial: true},
		{header: "bytes=10-", want: byteRange{10, 99}, partial: true},
		{header: "bytes=-5", want: byteRange{95, 99}, partial: true},
		{header: "bytes=-500", want: byteRange{0, 99}, partial: true},
		{header: "bytes=90-500", want: byteRange{90, 99}, partial: true},
		{header: "bytes=99-99", want: byteRange{99, 99}, partial: true},
		{header: "bytes=100-", invalid: true},
		{header: "bytes=200-300", invalid: true},
		{header: "bytes=-0", invalid: true},
		{header: "bytes=5-1"},     // ignored: last before first
		{header: "bytes=0-1,5-6"}, // ignored: several ranges
		{header: "items=0-1"},     // ignored: not bytes
		{header: "bytes=a-b"},     // ignored: not numbers
		{header: "bytes=5"},       // ignored: no dash
		{header: " bytes=1-2 ", want: byteRange{1, 2}, partial: true},
	}
	for _, tt := range tests {
		t.Run(tt.header, func(t *testing.T) {
			got, partial, err := parseRange(tt.header, size)
			if tt.invalid {
				var apiErr *awsapi.Error
				if !errors.As(err, &apiErr) || apiErr.Code != "InvalidRange" || apiErr.Status != 416 {
					t.Fatalf("err = %v, want InvalidRange 416", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("err = %v", err)
			}
			if partial != tt.partial || (partial && got != tt.want) {
				t.Fatalf("got %+v partial=%v, want %+v partial=%v", got, partial, tt.want, tt.partial)
			}
		})
	}
}

func TestParseRangeEmptyObject(t *testing.T) {
	for _, h := range []string{"bytes=0-", "bytes=-1"} {
		if _, _, err := parseRange(h, 0); err == nil {
			t.Errorf("%s on an empty object: want InvalidRange", h)
		}
	}
}

func TestContentRange(t *testing.T) {
	if got := (byteRange{2, 5}).contentRange(10); got != "bytes 2-5/10" {
		t.Fatalf("contentRange = %q", got)
	}
}
