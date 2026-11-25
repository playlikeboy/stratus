// Package awsapi holds what every emulated service shares: the fixed identity
// of the emulator, the AWS error shape and the JSON-protocol response writers.
package awsapi

import (
	"crypto/rand"
	"encoding/json"
	"fmt"
	"net/http"
)

const (
	DefaultAccount = "000000000000"
	DefaultRegion  = "eu-central-1"

	RequestIDHeader = "x-amzn-RequestId"
)

// Identity is the account and region every resource of one emulator lives in.
type Identity struct {
	Account string
	Region  string
}

// ARN builds an ARN of a resource in this identity.
func (i Identity) ARN(service, resource string) string {
	return "arn:aws:" + service + ":" + i.Region + ":" + i.Account + ":" + resource
}

// Error is an AWS service error as a client sees it.
type Error struct {
	Status int
	// Code is the error shape name, for example "QueueDoesNotExist".
	Code string
	// QueryCode is the legacy query-protocol code some services still send in
	// the x-amzn-query-error header; empty when the service has none.
	QueryCode string
	Message   string
}

func (e *Error) Error() string { return e.Code + ": " + e.Message }

// NewID returns a random UUID (version 4).
func NewID() string {
	var b [16]byte
	_, _ = rand.Read(b[:]) // crypto/rand.Read never returns an error
	b[6] = b[6]&0x0f | 0x40
	b[8] = b[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:16])
}

// WriteJSON writes a JSON-protocol response.
func WriteJSON(w http.ResponseWriter, contentType string, status int, v any) {
	body, err := json.Marshal(v)
	if err != nil {
		http.Error(w, "stratus: encoding the response: "+err.Error(), http.StatusInternalServerError)
		return
	}
	w.Header().Set("Content-Type", contentType)
	w.WriteHeader(status)
	_, _ = w.Write(body)
}

// WriteJSONError writes e the way the awsJson protocols do: the shape in
// "__type", prefixed with the service's namespace.
func WriteJSONError(w http.ResponseWriter, contentType, namespace string, e *Error) {
	if e.QueryCode != "" {
		fault := "Sender"
		if e.Status >= http.StatusInternalServerError {
			fault = "Receiver"
		}
		w.Header().Set("x-amzn-query-error", e.QueryCode+";"+fault)
	}
	WriteJSON(w, contentType, e.Status, map[string]string{
		"__type":  namespace + "#" + e.Code,
		"message": e.Message,
	})
}
