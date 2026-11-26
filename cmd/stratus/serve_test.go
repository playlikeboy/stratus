package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net"
	"net/http"
	"testing"
	"time"
)

// HTTPS answers over HTTP/2 every time, whichever server goroutine runs first.
// One http.Server serving both listeners configured HTTP/2 once, for the one
// that started first: when that was the plain one, after TLSConfig was set,
// HTTPS announced HTTP/2 in ALPN and dropped every connection that took it.
func TestServeAnswersHTTPSOverHTTP2AndHTTPOverHTTP1(t *testing.T) {
	cert, pemBytes, err := selfSigned([]string{"127.0.0.1"})
	if err != nil {
		t.Fatal(err)
	}
	roots := x509.NewCertPool()
	roots.AppendCertsFromPEM(pemBytes)
	client := &http.Client{Timeout: 3 * time.Second, Transport: &http.Transport{
		TLSClientConfig: &tls.Config{RootCAs: roots}, ForceAttemptHTTP2: true,
	}}
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { fmt.Fprint(w, r.Proto) })

	for i := range 30 {
		plain, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		secure, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			done <- serve(ctx, handler, plain, secure, &tls.Config{Certificates: []tls.Certificate{cert}, MinVersion: tls.VersionTLS12})
		}()

		for url, want := range map[string]string{
			"https://" + secure.Addr().String() + "/": "HTTP/2.0",
			"http://" + plain.Addr().String() + "/":   "HTTP/1.1",
		} {
			resp, err := client.Get(url)
			if err != nil {
				t.Fatalf("run %d: GET %s: %v", i, url, err)
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			if string(body) != want {
				t.Fatalf("run %d: %s answered over %s, want %s", i, url, body, want)
			}
		}
		cancel()
		if err := <-done; err != nil {
			t.Fatalf("run %d: serve after cancel: %v", i, err)
		}
		client.CloseIdleConnections()
	}
}
