package web

import (
	"strings"
	"testing"
	"time"
)

func TestDirectPartSize(t *testing.T) {
	cases := []struct {
		size      int64
		wantSize  int64
		wantCount int
	}{
		{1, 16 << 20, 1},
		{16 << 20, 16 << 20, 1},
		{16<<20 + 1, 16 << 20, 2},
		{6 << 30, 16 << 20, 384},
		{1 << 40, 105 << 20, 9987},
	}
	for _, c := range cases {
		ps, n := directPartSize(c.size)
		if ps != c.wantSize || n != c.wantCount {
			t.Errorf("size %d: got %d x %d, want %d x %d", c.size, ps, n, c.wantSize, c.wantCount)
		}
		if n > directMaxParts {
			t.Errorf("size %d: %d parts", c.size, n)
		}
	}
}

func TestDirectObjectKey(t *testing.T) {
	c := &directS3Config{mountPath: "/data", keyPrefix: "pre"}
	if k, ok := c.objectKey("/data/a b/c.mp4"); !ok || k != "pre/a b/c.mp4" {
		t.Errorf("got %q %v", k, ok)
	}
	for _, p := range []string{"/data", "/other/x", "/data/../x"} {
		if _, ok := c.objectKey(p); ok {
			t.Errorf("%s should not map to a key", p)
		}
	}
}

func TestDirectPresignShape(t *testing.T) {
	c := &directS3Config{scheme: "https", host: "s3.example.com", region: "r1", bucket: "b", accessKey: "AK", secretKey: "SK"}
	u := c.presign("PUT", "dir/a b.bin", time.Hour, map[string]string{"partNumber": "3", "uploadId": "x/y"})
	for _, want := range []string{"https://s3.example.com/b/dir/a%20b.bin?", "partNumber=3", "uploadId=x%2Fy", "X-Amz-Expires=3600", "X-Amz-Signature="} {
		if !strings.Contains(u, want) {
			t.Errorf("presigned URL lacks %q", want)
		}
	}
}
