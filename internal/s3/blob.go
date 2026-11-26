package s3

import (
	"bytes"
	"fmt"
	"hash"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync/atomic"
)

// blob is the bytes of an object or a part, kept in a file of the data
// directory rather than in memory. A blob never changes once written, and
// its file is removed only once nothing refers to the blob any more: a
// version, a copy sharing it, or a reader that took the object under the
// lock and serves it after releasing it.
type blob struct {
	path string // empty for an empty blob, which has no file
	size int64
}

var emptyBlob = &blob{}

// blobStore writes blobs as files of one directory.
type blobStore struct {
	dir  string
	next atomic.Uint64
}

func newBlobStore(dir string) *blobStore { return &blobStore{dir: dir} }

// put writes data as a new blob.
func (st *blobStore) put(data []byte) (*blob, error) {
	return st.write(func(f io.Writer) (int64, error) {
		n, err := f.Write(data)
		return int64(n), err
	})
}

// concat writes the blobs one after the other as a new blob, feeding the
// bytes to each hash as well.
func (st *blobStore) concat(parts []*blob, hashes ...hash.Hash) (*blob, error) {
	return st.write(func(f io.Writer) (int64, error) {
		dst := f
		if len(hashes) > 0 {
			ws := []io.Writer{f}
			for _, h := range hashes {
				ws = append(ws, h)
			}
			dst = io.MultiWriter(ws...)
		}
		var total int64
		for _, p := range parts {
			n, err := p.copyTo(dst, 0, p.size)
			total += n
			if err != nil {
				return total, err
			}
		}
		return total, nil
	})
}

func (st *blobStore) write(fill func(io.Writer) (int64, error)) (*blob, error) {
	path := filepath.Join(st.dir, strconv.FormatUint(st.next.Add(1), 36))
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("storing an object: %w", err)
	}
	n, err := fill(f)
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		_ = os.Remove(path)
		return nil, fmt.Errorf("storing an object: %w", err)
	}
	if n == 0 {
		_ = os.Remove(path)
		return emptyBlob, nil
	}
	b := &blob{path: path, size: n}
	runtime.AddCleanup(b, func(p string) { _ = os.Remove(p) }, path)
	return b, nil
}

// adopt returns the blob of a file a store in the same directory wrote before,
// as a snapshot names it, and keeps the store's names after it. The file is
// removed, as any blob's, once nothing refers to the blob.
func (st *blobStore) adopt(name string, size int64) (*blob, error) {
	if name == "" {
		return emptyBlob, nil
	}
	if filepath.Base(name) != name {
		return nil, fmt.Errorf("blob name %q is not a file of the store", name)
	}
	path := filepath.Join(st.dir, name)
	info, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if info.Size() != size {
		return nil, fmt.Errorf("blob %s holds %d bytes, the snapshot says %d", name, info.Size(), size)
	}
	if n, err := strconv.ParseUint(name, 36, 64); err == nil {
		for cur := st.next.Load(); n > cur && !st.next.CompareAndSwap(cur, n); cur = st.next.Load() {
		}
	}
	b := &blob{path: path, size: size}
	runtime.AddCleanup(b, func(p string) { _ = os.Remove(p) }, path)
	return b, nil
}

// sweep removes the files of the directory that keep does not name: those
// written after the snapshot a store was restored from.
func (st *blobStore) sweep(keep map[string]bool) error {
	entries, err := os.ReadDir(st.dir)
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() && !keep[e.Name()] {
			_ = os.Remove(filepath.Join(st.dir, e.Name()))
		}
	}
	return nil
}

// name is the file name of b in its store, empty for the empty blob.
func (b *blob) name() string {
	if b == nil || b.path == "" {
		return ""
	}
	return filepath.Base(b.path)
}

// copyTo writes n bytes of b from offset off to w.
func (b *blob) copyTo(w io.Writer, off, n int64) (int64, error) {
	if n == 0 {
		return 0, nil
	}
	f, err := os.Open(b.path)
	if err != nil {
		return 0, fmt.Errorf("reading an object: %w", err)
	}
	defer f.Close()
	written, err := io.Copy(w, io.NewSectionReader(f, off, n))
	// b must stay reachable, and its file with it, until the copy is done.
	runtime.KeepAlive(b)
	return written, err
}

// read returns n bytes of b from offset off.
func (b *blob) read(off, n int64) ([]byte, error) {
	var buf bytes.Buffer
	buf.Grow(int(n))
	if _, err := b.copyTo(&buf, off, n); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// bytes returns all of b.
func (b *blob) bytes() ([]byte, error) { return b.read(0, b.size) }
