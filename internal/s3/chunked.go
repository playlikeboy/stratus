package s3

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"strconv"
	"strings"
)

// maxChunkLine bounds a chunk header or trailer line: a hex size with a
// 64-character signature, or a checksum trailer, is far below it.
const maxChunkLine = 4096

// decodeAWSChunked decodes an aws-chunked body, the framing the SDKs use for
// streaming uploads:
//
//	<hex-size>[;chunk-signature=<sig>]\r\n<data>\r\n ... 0[;chunk-signature=<sig>]\r\n
//	[<trailer-name>:<value>\r\n ...]\r\n
//
// Chunk and trailer signatures are skipped, never verified. sizeHint, the
// announced decoded length, pre-sizes the result so the bytes are copied once;
// limit bounds the decoded size. Trailer names are returned lower-cased.
func decodeAWSChunked(r io.Reader, sizeHint, limit int64) ([]byte, map[string]string, error) {
	br := bufio.NewReaderSize(r, 64*1024)
	data := make([]byte, 0, max(0, min(sizeHint, limit, maxPrealloc)))
	for {
		line, err := readLine(br)
		if errors.Is(err, io.EOF) {
			return nil, nil, errIncompleteBody("The aws-chunked body ended before its final chunk.")
		}
		if err != nil {
			return nil, nil, err
		}
		size, err := chunkSize(line)
		if err != nil {
			return nil, nil, err
		}
		if size == 0 {
			trailers, err := readTrailers(br)
			return data, trailers, err
		}
		if int64(len(data))+size > limit {
			return nil, nil, errEntityTooLarge()
		}
		data, err = appendN(data, br, size)
		if err != nil {
			return nil, nil, err
		}
		if line, err := readLine(br); err != nil || line != "" {
			return nil, nil, errIncompleteBody("An aws-chunked chunk is not followed by CRLF.")
		}
	}
}

// chunkSize parses the hex size of a chunk header, ignoring its extensions.
func chunkSize(line string) (int64, error) {
	hexSize, _, _ := strings.Cut(line, ";")
	size, err := strconv.ParseInt(strings.TrimSpace(hexSize), 16, 64)
	if err != nil || size < 0 {
		return 0, errIncompleteBody("An aws-chunked chunk header is malformed: " + strconv.Quote(line) + ".")
	}
	return size, nil
}

// appendN appends exactly n bytes of r to data, growing it at most once.
func appendN(data []byte, r io.Reader, n int64) ([]byte, error) {
	// A chunk is taken in steps, so that a header announcing a huge chunk
	// reserves memory only as its bytes arrive.
	for n > 0 {
		step := min(n, maxPrealloc)
		start := len(data)
		data = growTo(data, start+int(step))
		if _, err := io.ReadFull(r, data[start:]); err != nil {
			return nil, errIncompleteBody("The aws-chunked body ended inside a chunk.")
		}
		n -= step
	}
	return data, nil
}

// growTo extends data to length n, reallocating with headroom when it lacks
// capacity, so that a body without a size hint is not copied per chunk.
func growTo(data []byte, n int) []byte {
	if n <= cap(data) {
		return data[:n]
	}
	grown := make([]byte, n, max(n, 2*cap(data)))
	copy(grown, data)
	return grown
}

// readTrailers reads the trailer lines after the final chunk up to the empty
// line; a body that simply ends after the final chunk has no trailers.
func readTrailers(br *bufio.Reader) (map[string]string, error) {
	trailers := map[string]string{}
	for {
		line, err := readLine(br)
		if errors.Is(err, io.EOF) {
			return trailers, nil
		}
		if err != nil {
			return nil, err
		}
		if line == "" {
			return trailers, nil
		}
		name, value, ok := strings.Cut(line, ":")
		if !ok {
			return nil, errIncompleteBody("An aws-chunked trailer is malformed: " + strconv.Quote(line) + ".")
		}
		trailers[strings.ToLower(strings.TrimSpace(name))] = strings.TrimSpace(value)
	}
}

// readLine reads one CRLF- (or LF-) terminated line without the terminator.
// It returns io.EOF only when the body ends before any byte of the line.
func readLine(br *bufio.Reader) (string, error) {
	var line []byte
	for {
		frag, err := br.ReadSlice('\n')
		line = append(line, frag...)
		if len(line) > maxChunkLine {
			return "", errIncompleteBody("An aws-chunked line is too long.")
		}
		switch {
		case err == nil:
			return string(bytes.TrimRight(line, "\r\n")), nil
		case errors.Is(err, bufio.ErrBufferFull):
			continue
		case errors.Is(err, io.EOF) && len(line) == 0:
			return "", io.EOF
		default:
			return "", errIncompleteBody("The aws-chunked body ended inside a line.")
		}
	}
}
