package backup

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/sirrobot01/decypharr/pkg/storage"
)

// NewDBExporter returns the DBExporter for st: each store is streamed into
// its own blob and the blob references go into the component entry.
func NewDBExporter(st *storage.Storage) DBExporter {
	return func(ctx context.Context, blobs BlobStore, p Progress) (ComponentEntry, error) {
		shas := map[string]string{}
		// Passes run one after another; each blob is finished before the
		// next pass starts so a repeated store replaces its first blob.
		var wait func() error
		var cur *io.PipeWriter
		open := func(store string) (io.Writer, error) {
			if wait != nil {
				if err := wait(); err != nil {
					return nil, err
				}
			}
			pr, pw := io.Pipe()
			cur = pw
			done := make(chan error, 1)
			go func() {
				ref, _, err := blobs.PutBlob(ctx, pr, CompressDefault)
				if err == nil {
					shas[store] = ref.SHA256
				}
				// Unblock the writer if PutBlob stopped reading early.
				_ = pr.CloseWithError(errOrClosed(err))
				done <- err
			}()
			var once sync.Once
			var result error
			// Later calls return the first result; the channel is sent to once.
			wait = func() error {
				once.Do(func() { result = <-done })
				return result
			}
			return &pipeStore{pw: pw}, nil
		}
		counts, err := st.ExportStores(open, func(store string, n int) {
			p.Report(ProgressUpdate{Component: ComponentDB, Phase: fmt.Sprintf("%s: %d records", store, n)})
		})
		if err != nil {
			// A failed pass leaves its pipe open; end it so PutBlob returns.
			if cur != nil {
				_ = cur.CloseWithError(err)
			}
			if wait != nil {
				_ = wait()
			}
			return ComponentEntry{}, err
		}
		if wait != nil {
			if err := wait(); err != nil {
				return ComponentEntry{}, err
			}
		}
		total := 0
		for _, n := range counts {
			total += n
		}
		return ComponentEntry{StoreBlobs: shas, Records: total}, ctx.Err()
	}
}

func errOrClosed(err error) error {
	if err != nil {
		return err
	}
	return io.ErrClosedPipe
}

// pipeStore closes the writing end when the store pass ends.
type pipeStore struct{ pw *io.PipeWriter }

func (p *pipeStore) Write(b []byte) (int, error) { return p.pw.Write(b) }
func (p *pipeStore) Close() error                { return p.pw.Close() }
