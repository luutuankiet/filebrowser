package indexing

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Object-store mounts report a placeholder date for every folder. A folder's
// date must become its newest child's once listed, and its parent's listing
// must keep showing that date afterwards.
func TestFolderModTimeFromNewestChild(t *testing.T) {
	idx, _, cleanup := setupTestIndex(t)
	defer cleanup()

	root := t.TempDir()
	child := filepath.Join(root, "album")
	if err := os.Mkdir(child, 0o755); err != nil {
		t.Fatal(err)
	}
	newest := time.Date(2024, 5, 6, 7, 8, 9, 0, time.UTC)
	older := newest.Add(-48 * time.Hour)
	for name, mt := range map[string]time.Time{"a.jpg": older, "b.jpg": newest} {
		p := filepath.Join(child, name)
		if err := os.WriteFile(p, []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chtimes(p, mt, mt); err != nil {
			t.Fatal(err)
		}
	}
	placeholder := time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC)
	if err := os.Chtimes(child, placeholder, placeholder); err != nil {
		t.Fatal(err)
	}

	dir, err := os.Open(child)
	if err != nil {
		t.Fatal(err)
	}
	defer dir.Close()
	stat, err := dir.Stat()
	if err != nil {
		t.Fatal(err)
	}
	info, err := idx.GetDirInfoCore(dir, stat, "/album/", Options{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !info.ModTime.Equal(newest) {
		t.Fatalf("listed folder date = %v, want newest child %v", info.ModTime, newest)
	}

	// What indexDirectory records after listing the folder.
	idx.SetFolderModTime("/album/", info.ModTime)

	// The parent listing still sees the placeholder stat date for the folder.
	fi := &mockFileInfo{name: "album", mode: os.ModeDir, modTime: placeholder, isDir: true}
	for _, opts := range []Options{{}, {SkipExtendedAttrs: true}} {
		item, _, _ := idx.processDirectoryItem(fi, "/", nil, opts, nil)
		if item == nil || !item.ModTime.Equal(newest) {
			t.Fatalf("parent listing folder date = %v, want %v", item, newest)
		}
	}

	// A real stat date newer than the recorded one wins.
	later := newest.Add(time.Hour)
	if got := idx.GetFolderModTimeForDisplay("/album/", later); !got.Equal(later) {
		t.Fatalf("GetFolderModTimeForDisplay = %v, want %v", got, later)
	}
	// An unrecorded folder keeps its stat date.
	if got := idx.GetFolderModTimeForDisplay("/other/", placeholder); !got.Equal(placeholder) {
		t.Fatalf("unrecorded folder date = %v, want %v", got, placeholder)
	}
}
