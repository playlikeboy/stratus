// Package gateway is the single front door of the emulator: it decides which
// service a request is for and hands it over. It never verifies a signature.
package gateway

import (
	"net/http"
	"net/url"
	"strings"

	"github.com/stratus-emulator/stratus/internal/awsapi"
)

// Service is one emulated AWS service.
type Service interface {
	http.Handler
	// Name is the service's name in a SigV4 credential scope, for example "sqs".
	Name() string
	// TargetPrefix is the prefix of the service's X-Amz-Target values, for
	// example "AmazonSQS."; empty for a service that has no such header.
	TargetPrefix() string
	// Reset drops all state.
	Reset()
}

// Gateway routes requests to services.
type Gateway struct {
	services []Service
	fallback Service
}

func New(services ...Service) *Gateway {
	return &Gateway{services: services}
}

// WithFallback names the service that gets the requests carrying neither a
// target nor a credential scope: anonymous and hand-written requests, which
// only a REST service such as S3 can make sense of.
func (g *Gateway) WithFallback(s Service) *Gateway {
	g.fallback = s
	return g
}

// Services returns the registered services' names.
func (g *Gateway) Services() []string {
	names := make([]string, 0, len(g.services))
	for _, s := range g.services {
		names = append(names, s.Name())
	}
	return names
}

// Reset drops the state of every service.
func (g *Gateway) Reset() {
	for _, s := range g.services {
		s.Reset()
	}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set(awsapi.RequestIDHeader, awsapi.NewID())

	target := r.Header.Get("X-Amz-Target")
	scope := credentialScopeService(r)
	if s := g.resolve(target, scope); s != nil {
		s.ServeHTTP(w, r)
		return
	}
	awsapi.WriteJSON(w, "application/json", http.StatusNotImplemented, map[string]string{
		"__type": "UnsupportedService",
		"message": "stratus: no service handles this request (X-Amz-Target=" + quote(target) +
			", credential scope service=" + quote(scope) + ")",
	})
}

// resolve picks the service: the X-Amz-Target header wins, because it is exact;
// the credential scope is the fallback for protocols that have no target.
func (g *Gateway) resolve(target, scope string) Service {
	if target != "" {
		for _, s := range g.services {
			if p := s.TargetPrefix(); p != "" && strings.HasPrefix(target, p) {
				return s
			}
		}
	}
	if scope != "" {
		for _, s := range g.services {
			if s.Name() == scope {
				return s
			}
		}
	}
	if target == "" && scope == "" {
		return g.fallback
	}
	return nil
}

// credentialScopeService reads the service out of a SigV4 credential scope
// ("<key>/<date>/<region>/<service>/aws4_request"), taken from the
// Authorization header or, for a presigned URL, from X-Amz-Credential.
func credentialScopeService(r *http.Request) string {
	if s := scopeService(credentialOf(r.Header.Get("Authorization"))); s != "" {
		return s
	}
	if raw := r.URL.RawQuery; strings.Contains(raw, "X-Amz-Credential=") {
		if q, err := url.ParseQuery(raw); err == nil {
			return scopeService(q.Get("X-Amz-Credential"))
		}
	}
	return ""
}

func credentialOf(authorization string) string {
	_, rest, ok := strings.Cut(authorization, "Credential=")
	if !ok {
		return ""
	}
	if i := strings.IndexAny(rest, ", "); i >= 0 {
		return rest[:i]
	}
	return rest
}

func scopeService(credential string) string {
	parts := strings.Split(credential, "/")
	if len(parts) < 5 {
		return ""
	}
	return parts[len(parts)-2]
}

func quote(s string) string {
	if s == "" {
		return "none"
	}
	return `"` + s + `"`
}
