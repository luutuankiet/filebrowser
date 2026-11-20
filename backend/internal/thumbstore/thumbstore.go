// Package thumbstore keeps generated previews (thumbnails and the resized
// image the viewer opens) in an S3-compatible bucket, so they survive a
// restart of a server whose disk is thrown away, and lets the browser fetch
// them from the bucket with a presigned URL instead of through the server.
//
// Off unless FB_THUMBSTORE_BUCKET is set. Settings, all environment variables:
//
//	FB_THUMBSTORE_ENDPOINT   S3 host, e.g. s3.us-west-004.backblazeb2.com
//	FB_THUMBSTORE_REGION     signing region, e.g. us-west-004
//	FB_THUMBSTORE_BUCKET     bucket for the previews
//	FB_THUMBSTORE_PREFIX     key prefix inside the bucket (optional)
//	FB_THUMBSTORE_KEY_ID     access key id
//	FB_THUMBSTORE_SECRET     secret access key
//	FB_THUMBSTORE_REDIRECT   "false" serves stored previews through the server
//	                         instead of a 302 to the bucket (default true)
//
// An object's key is the file's source name, its path in the source, its size
// and its modification time, so an edited file gets a new key and the old
// preview is simply never asked for again. Presence is learnt with one list
// request per folder, kept in memory, so a folder of 300 photos costs one
// round trip to the bucket, not 300.
package thumbstore

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/xml"
	"fmt"
	"io"
	"net/http"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type config struct {
	endpoint, region, bucket, prefix, keyID, secret string
	redirect                                        bool
}

var (
	cfg     *config
	cfgOnce sync.Once
	client  = &http.Client{Timeout: 30 * time.Second}

	dirsMu sync.Mutex
	dirs   = map[string]*dirEntry{}
)

// How long a folder's list of stored previews is trusted before it is fetched
// again (a pre-generation job may be filling the store meanwhile).
const dirTTL = 10 * time.Minute

type dirEntry struct {
	mu   sync.Mutex
	at   time.Time
	keys map[string]bool
}

func load() *config {
	cfgOnce.Do(func() {
		if os.Getenv("FB_THUMBSTORE_BUCKET") == "" {
			return
		}
		cfg = &config{
			endpoint: strings.TrimPrefix(strings.TrimPrefix(os.Getenv("FB_THUMBSTORE_ENDPOINT"), "https://"), "http://"),
			region:   os.Getenv("FB_THUMBSTORE_REGION"),
			bucket:   os.Getenv("FB_THUMBSTORE_BUCKET"),
			prefix:   strings.Trim(os.Getenv("FB_THUMBSTORE_PREFIX"), "/"),
			keyID:    os.Getenv("FB_THUMBSTORE_KEY_ID"),
			secret:   os.Getenv("FB_THUMBSTORE_SECRET"),
			redirect: os.Getenv("FB_THUMBSTORE_REDIRECT") != "false",
		}
		if cfg.region == "" {
			cfg.region = "us-east-1"
		}
	})
	return cfg
}

// Enabled reports whether a store is configured.
func Enabled() bool { return load() != nil }

// Key is the object key for one preview of one file.
func Key(source, path string, size int64, modTime time.Time, previewSize string, percentage int) string {
	k := fmt.Sprintf("%s/%s@%d-%d-%s", source, strings.TrimPrefix(path, "/"), size, modTime.Unix(), previewSize)
	if percentage > 0 {
		k += fmt.Sprintf("-p%d", percentage)
	}
	k += ".jpg"
	if c := load(); c != nil && c.prefix != "" {
		k = c.prefix + "/" + k
	}
	return k
}

// Has reports whether the store holds key, listing its folder at most once
// per dirTTL.
func Has(ctx context.Context, key string) bool {
	if load() == nil {
		return false
	}
	dir := key[:strings.LastIndex(key, "/")+1]
	dirsMu.Lock()
	e := dirs[dir]
	if e == nil {
		e = &dirEntry{}
		dirs[dir] = e
	}
	dirsMu.Unlock()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.keys == nil || time.Since(e.at) > dirTTL {
		keys, err := List(ctx, dir, "/")
		if err != nil {
			return false
		}
		e.keys, e.at = map[string]bool{}, time.Now()
		for _, k := range keys {
			e.keys[k] = true
		}
	}
	return e.keys[key]
}

func remember(key string) {
	dir := key[:strings.LastIndex(key, "/")+1]
	dirsMu.Lock()
	e := dirs[dir]
	dirsMu.Unlock()
	if e != nil {
		e.mu.Lock()
		if e.keys != nil {
			e.keys[key] = true
		}
		e.mu.Unlock()
	}
}

// Redirect answers with a 302 to the stored preview when the store holds it
// and redirects are on. It returns false when the caller must serve it.
func Redirect(w http.ResponseWriter, r *http.Request, key string) bool {
	c := load()
	if c == nil || !c.redirect || !Has(r.Context(), key) {
		return false
	}
	// The URL is signed with the start of the current UTC day as its time, so
	// it is the same string all day and the browser's cache keeps hitting.
	w.Header().Set("Cache-Control", "private, max-age=3600")
	http.Redirect(w, r, presign(c, "GET", key, nil, true), http.StatusFound)
	return true
}

// Get fetches a stored preview, for when the server must serve the bytes.
func Get(ctx context.Context, key string) ([]byte, bool) {
	c := load()
	if c == nil || !Has(ctx, key) {
		return nil, false
	}
	req, _ := http.NewRequestWithContext(ctx, "GET", presign(c, "GET", key, nil, false), nil)
	resp, err := client.Do(req)
	if err != nil {
		return nil, false
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, false
	}
	b, err := io.ReadAll(resp.Body)
	return b, err == nil
}

// Put stores a preview. Previews never change under a key, so the object is
// marked cacheable for a year.
func Put(ctx context.Context, key string, data []byte) error {
	c := load()
	if c == nil {
		return nil
	}
	req, _ := http.NewRequestWithContext(ctx, "PUT", presign(c, "PUT", key, nil, false), bytes.NewReader(data))
	req.ContentLength = int64(len(data))
	req.Header.Set("Content-Type", "image/jpeg")
	req.Header.Set("Cache-Control", "private, max-age=31536000, immutable")
	resp, err := client.Do(req)
	if err != nil {
		return fmt.Errorf("thumbstore put: %s", strings.SplitN(err.Error(), "?", 2)[0])
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("thumbstore put %s: %s", key, resp.Status)
	}
	remember(key)
	return nil
}

// PutAsync stores a preview in the background, so a viewer is not kept
// waiting on the bucket.
func PutAsync(key string, data []byte) {
	if load() == nil {
		return
	}
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
		defer cancel()
		_ = Put(ctx, key, data)
	}()
}

// List returns every key under prefix; with delimiter "/" only that folder's
// own objects, not those of its subfolders.
func List(ctx context.Context, prefix, delimiter string) ([]string, error) {
	c := load()
	if c == nil {
		return nil, nil
	}
	var keys []string
	token := ""
	for {
		q := map[string]string{"list-type": "2", "prefix": prefix}
		if delimiter != "" {
			q["delimiter"] = delimiter
		}
		if token != "" {
			q["continuation-token"] = token
		}
		req, _ := http.NewRequestWithContext(ctx, "GET", presign(c, "GET", "", q, false), nil)
		resp, err := client.Do(req)
		if err != nil {
			return nil, fmt.Errorf("thumbstore list: %s", strings.SplitN(err.Error(), "?", 2)[0])
		}
		var res struct {
			Contents  []struct{ Key string }
			Truncated bool   `xml:"IsTruncated"`
			Next      string `xml:"NextContinuationToken"`
		}
		body, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			return nil, fmt.Errorf("thumbstore list %s: %s", prefix, resp.Status)
		}
		if err := xml.Unmarshal(body, &res); err != nil {
			return nil, err
		}
		for _, o := range res.Contents {
			keys = append(keys, o.Key)
		}
		if !res.Truncated || res.Next == "" {
			return keys, nil
		}
		token = res.Next
	}
}

func hm(k []byte, s string) []byte {
	m := hmac.New(sha256.New, k)
	m.Write([]byte(s))
	return m.Sum(nil)
}

// enc is S3's URI encoding: everything but unreserved characters, with '/'
// kept in object paths.
func enc(s string, keepSlash bool) string {
	var b strings.Builder
	for _, c := range []byte(s) {
		if (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9') ||
			c == '-' || c == '_' || c == '.' || c == '~' || (keepSlash && c == '/') {
			b.WriteByte(c)
		} else {
			fmt.Fprintf(&b, "%%%02X", c)
		}
	}
	return b.String()
}

// presign returns a SigV4 query-signed path-style URL. With stable set, the
// signing time is the start of the UTC day and the URL lives two days, so
// every URL signed on one day is identical and valid until the next day ends.
func presign(c *config, method, key string, extra map[string]string, stable bool) string {
	now := time.Now().UTC()
	ttl := 15 * time.Minute
	if stable {
		now = now.Truncate(24 * time.Hour)
		ttl = 48 * time.Hour
	}
	date, stamp := now.Format("20060102"), now.Format("20060102T150405Z")
	scope := date + "/" + c.region + "/s3/aws4_request"
	q := map[string]string{
		"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
		"X-Amz-Credential":    c.keyID + "/" + scope,
		"X-Amz-Date":          stamp,
		"X-Amz-Expires":       fmt.Sprint(int(ttl.Seconds())),
		"X-Amz-SignedHeaders": "host",
	}
	for k, v := range extra {
		q[k] = v
	}
	names := make([]string, 0, len(q))
	for k := range q {
		names = append(names, k)
	}
	sort.Strings(names)
	parts := make([]string, len(names))
	for i, k := range names {
		parts[i] = enc(k, false) + "=" + enc(q[k], false)
	}
	cq := strings.Join(parts, "&")
	p := "/" + c.bucket + "/" + enc(key, true)
	creq := strings.Join([]string{method, p, cq, "host:" + c.endpoint + "\n", "host", "UNSIGNED-PAYLOAD"}, "\n")
	h := sha256.Sum256([]byte(creq))
	sts := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(h[:])
	sk := hm(hm(hm(hm([]byte("AWS4"+c.secret), date), c.region), "s3"), "aws4_request")
	return "https://" + c.endpoint + p + "?" + cq + "&X-Amz-Signature=" + hex.EncodeToString(hm(sk, sts))
}
