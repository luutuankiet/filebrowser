package cmd

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gtsteffaniak/filebrowser/backend/internal/preview"
	"github.com/gtsteffaniak/filebrowser/backend/internal/thumbstore"
	"github.com/gtsteffaniak/filebrowser/backend/pkg/indexing/iteminfo"
	"github.com/gtsteffaniak/filebrowser/backend/pkg/settings"
)

// ThumbsCmd fills the bucket preview store (internal/thumbstore) for one
// source ahead of time. It is idempotent: a preview already in the store is
// skipped after one list request per folder, so an interrupted run resumes by
// running it again.
type ThumbsCmd struct {
	Source  string `arg:"" help:"Source name, as in the server config"`
	Sizes   string `default:"small" help:"Comma-separated preview sizes to make: small (grid thumbnails), large (viewer)"`
	Workers int    `default:"0" help:"Previews made at once (default: numImageProcessors from the config)"`
	Under   string `default:"" help:"Only this folder of the source, e.g. /2019"`
}

func (t *ThumbsCmd) Run(globals *Globals) error {
	if !thumbstore.Enabled() {
		return fmt.Errorf("FB_THUMBSTORE_BUCKET is not set")
	}
	settings.Initialize(globals.Config)
	src, ok := settings.Config.Server.NameToSource[t.Source]
	if !ok {
		return fmt.Errorf("source %q not in config", t.Source)
	}
	workers := t.Workers
	if workers < 1 {
		workers = settings.Config.Server.NumImageProcessors
	}
	tmp, err := os.MkdirTemp("", "thumbs-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	if err := preview.StartPreviewGenerator(workers, tmp); err != nil {
		return err
	}
	sizes := strings.Split(t.Sizes, ",")
	root := filepath.Join(src.Path, filepath.FromSlash(t.Under))

	var done, made, skipped, failed atomic.Int64
	start := time.Now()
	jobs := make(chan iteminfo.ExtendedFileInfo, workers*4)
	var wg sync.WaitGroup
	for i := 0; i < workers*2; i++ { // twice: half of them wait on the bucket
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range jobs {
				for _, size := range sizes {
					key := thumbstore.Key(f.Source, f.Path, f.Size, f.ModTime, size, 0)
					if thumbstore.Has(context.Background(), key) {
						skipped.Add(1)
						continue
					}
					ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
					data, err := preview.GeneratePreviewWithMD5(ctx, f, size, "", 0, fmt.Sprintf("pregen-%x", key))
					if err == nil {
						err = thumbstore.Put(ctx, key, data)
					}
					cancel()
					if err != nil {
						failed.Add(1)
						fmt.Fprintf(os.Stderr, "fail %s %s: %v\n", f.Path, size, err)
						continue
					}
					made.Add(1)
				}
				if n := done.Add(1); n%100 == 0 {
					fmt.Printf("%d files, %d made, %d already stored, %d failed, %.0f s\n",
						n, made.Load(), skipped.Load(), failed.Load(), time.Since(start).Seconds())
				}
			}
		}()
	}
	err = filepath.WalkDir(root, func(p string, de fs.DirEntry, err error) error {
		if err != nil {
			fmt.Fprintf(os.Stderr, "walk %s: %v\n", p, err)
			return nil
		}
		if de.IsDir() || strings.HasPrefix(de.Name(), "._") {
			return nil
		}
		info, err := de.Info()
		if err != nil {
			return nil
		}
		rel, _ := filepath.Rel(src.Path, p)
		f := iteminfo.ExtendedFileInfo{Source: t.Source, RealPath: p}
		f.Name, f.Size, f.ModTime = de.Name(), info.Size(), info.ModTime()
		f.Path = "/" + filepath.ToSlash(rel)
		f.DetectType(p, false)
		ext := strings.ToLower(filepath.Ext(f.Name))
		// Same rule as the preview endpoint: small images are sent as they
		// are, so they need no stored preview.
		if strings.HasPrefix(f.Type, "image") && !iteminfo.ResizableImageTypes[ext] && ext != ".heic" && ext != ".heif" {
			return nil
		}
		if !strings.HasPrefix(f.Type, "image") && !strings.HasPrefix(f.Type, "video") {
			return nil
		}
		f.HasPreview = true
		jobs <- f
		return nil
	})
	close(jobs)
	wg.Wait()
	fmt.Printf("done: %d files, %d made, %d already stored, %d failed, %.0f s\n",
		done.Load(), made.Load(), skipped.Load(), failed.Load(), time.Since(start).Seconds())
	return err
}
