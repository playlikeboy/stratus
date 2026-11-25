package s3

import (
	"encoding/xml"
	"errors"
	"net/http"
	"strings"
	"time"

	"github.com/ODudek/stratus/internal/awsapi"
)

const (
	s3Namespace   = "http://s3.amazonaws.com/doc/2006-03-01/"
	timeFormatXML = "2006-01-02T15:04:05.000Z"
)

func xmlTime(t time.Time) string { return t.UTC().Format(timeFormatXML) }

func xmlName(local string) xml.Name { return xml.Name{Space: s3Namespace, Local: local} }

// writeXML writes a successful XML response.
func writeXML(w http.ResponseWriter, v any) {
	body, err := xml.Marshal(v)
	if err != nil {
		writeError(w, nil, err)
		return
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(body)
}

// errorBody is the S3 error document.
type errorBody struct {
	XMLName   xml.Name `xml:"Error"`
	Code      string   `xml:"Code"`
	Message   string   `xml:"Message"`
	Resource  string   `xml:"Resource,omitempty"`
	RequestID string   `xml:"RequestId"`
}

// writeError writes err in the S3 error shape; an error that is not an
// awsapi.Error is an InternalError. A HEAD response carries the status only.
func writeError(w http.ResponseWriter, r *http.Request, err error) {
	var apiErr *awsapi.Error
	if !errors.As(err, &apiErr) {
		apiErr = s3Error(http.StatusInternalServerError, "InternalError", err.Error())
	}
	requestID := w.Header().Get(awsapi.RequestIDHeader)
	w.Header().Set("x-amz-request-id", requestID)
	if r != nil && r.Method == http.MethodHead {
		w.WriteHeader(apiErr.Status)
		return
	}
	body := errorBody{Code: apiErr.Code, Message: apiErr.Message, RequestID: requestID}
	if r != nil {
		body.Resource = r.URL.EscapedPath()
	}
	out, _ := xml.Marshal(body) // plain strings always marshal
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(apiErr.Status)
	_, _ = w.Write([]byte(xml.Header))
	_, _ = w.Write(out)
}

// readXML reads and verifies a small XML request body and decodes it into v.
func readXML(r *http.Request, v any) error {
	p, err := readPayload(r, maxXMLBody)
	if err != nil {
		return err
	}
	if err := xml.Unmarshal(p.data, v); err != nil {
		return errMalformedXML()
	}
	return nil
}

// checksumFields are the checksum elements several results carry.
type checksumFields struct {
	ChecksumCRC32     string `xml:"ChecksumCRC32,omitempty"`
	ChecksumCRC32C    string `xml:"ChecksumCRC32C,omitempty"`
	ChecksumCRC64NVME string `xml:"ChecksumCRC64NVME,omitempty"`
	ChecksumSHA1      string `xml:"ChecksumSHA1,omitempty"`
	ChecksumSHA256    string `xml:"ChecksumSHA256,omitempty"`
	ChecksumType      string `xml:"ChecksumType,omitempty"`
}

func checksumXML(algorithm, value, typ string) checksumFields {
	var f checksumFields
	switch algorithm {
	case algoCRC32:
		f.ChecksumCRC32 = value
	case algoCRC32C:
		f.ChecksumCRC32C = value
	case algoCRC64NVME:
		f.ChecksumCRC64NVME = value
	case algoSHA1:
		f.ChecksumSHA1 = value
	case algoSHA256:
		f.ChecksumSHA256 = value
	default:
		return f
	}
	f.ChecksumType = typ
	return f
}

type owner struct {
	ID          string `xml:"ID"`
	DisplayName string `xml:"DisplayName,omitempty"`
}

func (s *Service) owner() owner { return owner{ID: s.identity.Account, DisplayName: "stratus"} }

type commonPrefix struct {
	Prefix string `xml:"Prefix"`
}

func commonPrefixes(prefixes []string, enc func(string) string) []commonPrefix {
	out := make([]commonPrefix, len(prefixes))
	for i, p := range prefixes {
		out[i] = commonPrefix{Prefix: enc(p)}
	}
	return out
}

// keyEncoder returns the encoding a listing applies to keys and prefixes:
// none, or the URL encoding EncodingType=url asks for.
func keyEncoder(encodingType string) (func(string) string, error) {
	switch strings.ToLower(encodingType) {
	case "":
		return func(s string) string { return s }, nil
	case "url":
		return eventKey, nil
	}
	return nil, errInvalidArgument("Invalid Encoding Method specified in Request")
}
