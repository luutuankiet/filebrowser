package web

// Direct S3 transfers: an opt-in mode for sources that are an rclone mount of
// an S3-compatible bucket (written for Backblaze B2). File bytes move between
// the browser and the bucket on presigned URLs, so the server never carries
// them:
//
//   - downloads answer 302 to a presigned GET (Range keeps working, so video
//     seek does too);
//   - uploads are S3 multipart uploads whose parts the browser PUTs straight to
//     the bucket; the server only creates, presigns, completes or aborts, then
//     tells rclone and the index that the file exists.
//
// Off unless FILEBROWSER_DIRECT_S3_BUCKET, _ACCESS_KEY_ID, _SECRET_ACCESS_KEY,
// _ENDPOINT and _MOUNT_PATH are set. Presigned URLs are bearer tokens: never
// log them.

import (
	"bytes"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gtsteffaniak/filebrowser/backend/internal/activity"
	"github.com/gtsteffaniak/filebrowser/backend/internal/adapters/fs/files"
	"github.com/gtsteffaniak/filebrowser/backend/internal/state"
	"github.com/gtsteffaniak/filebrowser/backend/internal/utils"
	"github.com/gtsteffaniak/filebrowser/backend/pkg/indexing"
	"github.com/gtsteffaniak/go-logger/logger"
)

type directS3Config struct {
	scheme, host, region, bucket string
	accessKey, secretKey         string
	mountPath, keyPrefix         string
	rcloneRC                     string
	ttl                          time.Duration
}

// directS3 is nil when the feature is off.
var directS3 = loadDirectS3Config()

const (
	directMinPartSize = 16 << 20
	directMaxParts    = 10000
	directMaxPartSize = 5 << 30
)

var directHTTP = &http.Client{Timeout: 5 * time.Minute}

func loadDirectS3Config() *directS3Config {
	env := func(k string) string { return strings.TrimSpace(os.Getenv("FILEBROWSER_DIRECT_S3_" + k)) }
	c := &directS3Config{
		bucket:    env("BUCKET"),
		accessKey: env("ACCESS_KEY_ID"),
		secretKey: env("SECRET_ACCESS_KEY"),
		mountPath: env("MOUNT_PATH"),
		keyPrefix: strings.Trim(env("KEY_PREFIX"), "/"),
		rcloneRC:  strings.TrimRight(env("RCLONE_RC"), "/"),
		region:    env("REGION"),
		ttl:       time.Hour,
	}
	endpoint := env("ENDPOINT")
	if c.bucket == "" || c.accessKey == "" || c.secretKey == "" || c.mountPath == "" || endpoint == "" {
		return nil
	}
	u, err := url.Parse(endpoint)
	if err != nil || u.Host == "" {
		logger.Errorf("direct S3 transfers disabled: bad FILEBROWSER_DIRECT_S3_ENDPOINT")
		return nil
	}
	c.scheme, c.host = u.Scheme, u.Host
	if c.region == "" { // s3.<region>.backblazeb2.com
		if p := strings.Split(c.host, "."); len(p) > 2 && p[0] == "s3" {
			c.region = p[1]
		} else {
			c.region = "us-east-1"
		}
	}
	if v := env("URL_TTL"); v != "" {
		if d, err := time.ParseDuration(v); err == nil && d > 0 && d <= 7*24*time.Hour {
			c.ttl = d
		}
	}
	c.mountPath = filepath.Clean(c.mountPath)
	logger.Infof("direct S3 transfers enabled for files under %s", c.mountPath)
	return c
}

// relPath is the file's path relative to the mount root ("" for the root).
func (c *directS3Config) relPath(realPath string) (string, bool) {
	rel, err := filepath.Rel(c.mountPath, filepath.Clean(realPath))
	if err != nil || rel == ".." || strings.HasPrefix(rel, "../") {
		return "", false
	}
	if rel == "." {
		rel = ""
	}
	return filepath.ToSlash(rel), true
}

func (c *directS3Config) objectKey(realPath string) (string, bool) {
	rel, ok := c.relPath(realPath)
	if !ok || rel == "" {
		return "", false
	}
	if c.keyPrefix != "" {
		return c.keyPrefix + "/" + rel, true
	}
	return rel, true
}

func s3Encode(s string, keepSlash bool) string {
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

func hmacSHA256(k []byte, s string) []byte {
	m := hmac.New(sha256.New, k)
	m.Write([]byte(s))
	return m.Sum(nil)
}

// presign returns a SigV4 query-signed path-style URL for one object.
func (c *directS3Config) presign(method, key string, ttl time.Duration, extra map[string]string) string {
	now := time.Now().UTC()
	date, stamp := now.Format("20060102"), now.Format("20060102T150405Z")
	scope := date + "/" + c.region + "/s3/aws4_request"
	q := map[string]string{
		"X-Amz-Algorithm":     "AWS4-HMAC-SHA256",
		"X-Amz-Credential":    c.accessKey + "/" + scope,
		"X-Amz-Date":          stamp,
		"X-Amz-Expires":       strconv.Itoa(int(ttl.Seconds())),
		"X-Amz-SignedHeaders": "host",
	}
	for k, v := range extra {
		q[k] = v
	}
	keys := make([]string, 0, len(q))
	for k := range q {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	parts := make([]string, len(keys))
	for i, k := range keys {
		parts[i] = s3Encode(k, false) + "=" + s3Encode(q[k], false)
	}
	cq := strings.Join(parts, "&")
	p := "/" + c.bucket + "/" + s3Encode(key, true)
	creq := strings.Join([]string{method, p, cq, "host:" + c.host + "\n", "host", "UNSIGNED-PAYLOAD"}, "\n")
	h := sha256.Sum256([]byte(creq))
	sts := "AWS4-HMAC-SHA256\n" + stamp + "\n" + scope + "\n" + hex.EncodeToString(h[:])
	k := hmacSHA256(hmacSHA256(hmacSHA256(hmacSHA256([]byte("AWS4"+c.secretKey), date), c.region), "s3"), "aws4_request")
	return c.scheme + "://" + c.host + p + "?" + cq + "&X-Amz-Signature=" + hex.EncodeToString(hmacSHA256(k, sts))
}

// s3Do runs one server-side S3 call (create/complete/abort/head) on a
// presigned URL. Errors never include the URL.
func (c *directS3Config) s3Do(method, key string, q map[string]string, body []byte, contentType string) (int, http.Header, []byte, error) {
	var rd io.Reader
	if body != nil {
		rd = bytes.NewReader(body)
	}
	req, err := http.NewRequest(method, c.presign(method, key, 15*time.Minute, q), rd)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("s3 %s: bad request", method)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}
	resp, err := directHTTP.Do(req)
	if err != nil {
		return 0, nil, nil, fmt.Errorf("s3 %s failed: %T", method, err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 1<<20))
	return resp.StatusCode, resp.Header, b, nil
}

func contentTypeFor(name string) string {
	return mime.TypeByExtension(strings.ToLower(filepath.Ext(name)))
}

// directDownload answers a GET for a file under the mount with a 302 to a
// presigned URL. It returns false (serve normally) for fetch()/XHR callers:
// a cross-origin redirect followed by fetch() would need CORS for Origin null.
func directDownload(w http.ResponseWriter, r *http.Request, realPath, displayFileName string, forceInline bool) bool {
	c := directS3
	if c == nil || r.Method != http.MethodGet || r.Header.Get("Sec-Fetch-Dest") == "empty" {
		return false
	}
	key, ok := c.objectKey(realPath)
	if !ok {
		return false
	}
	if st, err := os.Stat(realPath); err != nil || st.IsDir() {
		return false
	}
	disposition := "attachment"
	if forceInline || r.URL.Query().Get("inline") == "true" {
		disposition = "inline"
	}
	q := map[string]string{
		"response-content-disposition": fmt.Sprintf("%s; filename=%q; filename*=utf-8''%s",
			disposition, toASCIIFilename(displayFileName), url.PathEscape(displayFileName)),
	}
	if ct := contentTypeFor(displayFileName); ct != "" {
		q["response-content-type"] = ct
	}
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Referrer-Policy", "no-referrer")
	http.Redirect(w, r, c.presign(http.MethodGet, key, c.ttl, q), http.StatusFound)
	return true
}

// ---- uploads ----

func registerDirectTransferRoutes(api *http.ServeMux) {
	api.HandleFunc("POST /transfers/uploads", withUser(directUploadCreateHandler))
	api.HandleFunc("POST /transfers/uploads/parts", withUser(directUploadPartsHandler))
	api.HandleFunc("POST /transfers/uploads/complete", withUser(directUploadCompleteHandler))
	api.HandleFunc("DELETE /transfers/uploads", withUser(directUploadAbortHandler))
}

type directUploadRequest struct {
	Source      string `json:"source"`
	Path        string `json:"path"`
	Size        int64  `json:"size"`
	Overwrite   bool   `json:"overwrite"`
	UploadID    string `json:"uploadId"`
	PartNumbers []int  `json:"partNumbers"`
	Parts       []struct {
		PartNumber int    `json:"partNumber"`
		ETag       string `json:"etag"`
	} `json:"parts"`
}

type directTarget struct {
	source, path, fullIndexPath, realPath, rel, key string
	exists                                          bool
}

func directPartSize(size int64) (int64, int) {
	ps := (size + directMaxParts - 1) / directMaxParts
	ps = (ps + (1<<20 - 1)) &^ (1<<20 - 1)
	if ps < directMinPartSize {
		ps = directMinPartSize
	}
	n := int((size + ps - 1) / ps)
	if n < 1 {
		n = 1
	}
	return ps, n
}

// directResolve applies the same checks as POST /api/resources and returns
// the object key for source+path. Every upload call re-derives it, so a client
// can never aim a part at another key.
func directResolve(r *http.Request, d *Context) (*directUploadRequest, *directTarget, int, error) {
	c := directS3
	if c == nil {
		return nil, nil, http.StatusNotFound, fmt.Errorf("direct transfers are off")
	}
	if d.Share.Hash != "" {
		return nil, nil, http.StatusForbidden, fmt.Errorf("direct transfers are not available on shares")
	}
	var req directUploadRequest
	if err := json.NewDecoder(io.LimitReader(r.Body, 4<<20)).Decode(&req); err != nil {
		return nil, nil, http.StatusBadRequest, fmt.Errorf("invalid body")
	}
	cleanPath, err := utils.SanitizePath(req.Path)
	if err != nil {
		return nil, nil, http.StatusBadRequest, err
	}
	idx := indexing.GetIndex(req.Source)
	if idx == nil {
		return nil, nil, http.StatusNotFound, fmt.Errorf("source %s not found", req.Source)
	}
	if idx.Config.ReadOnly {
		return nil, nil, http.StatusForbidden, fmt.Errorf("source is read-only")
	}
	userscope, err := d.User.GetScopeForSourceName(req.Source)
	if err != nil {
		return nil, nil, http.StatusForbidden, err
	}
	fullIndexPath := utils.JoinScopedIndexPath(strings.TrimRight(userscope, "/"), cleanPath)
	realPath, _, _ := idx.GetRealPath(fullIndexPath)
	st, statErr := os.Stat(realPath)
	exists := statErr == nil
	if exists && st.IsDir() {
		return nil, nil, http.StatusConflict, fmt.Errorf("a folder exists at that path")
	}
	perms, err := effectiveFilePerms(d, req.Source)
	if err != nil {
		return nil, nil, http.StatusForbidden, err
	}
	if status, gateErr := resourcePostPermCheck(exists, true, perms); gateErr != nil {
		return nil, nil, status, gateErr
	}
	if !state.AccessPermitted(idx.Path, utils.IndexPathFromNormalized(fullIndexPath, true), d.User.Username) {
		return nil, nil, http.StatusForbidden, fmt.Errorf("access denied to path %s", cleanPath)
	}
	rel, ok := c.relPath(realPath)
	key, ok2 := c.objectKey(realPath)
	if !ok || !ok2 {
		// Not under the mount: the client falls back to a normal upload.
		return nil, nil, http.StatusNotFound, fmt.Errorf("path is not on the direct S3 mount")
	}
	return &req, &directTarget{req.Source, cleanPath, fullIndexPath, realPath, rel, key, exists}, 0, nil
}

func directJSON(w http.ResponseWriter, v any) (int, error) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	return http.StatusOK, json.NewEncoder(w).Encode(v)
}

func directUploadCreateHandler(w http.ResponseWriter, r *http.Request, d *Context) (int, error) {
	req, t, status, err := directResolve(r, d)
	if err != nil {
		return status, err
	}
	if t.exists && !req.Overwrite {
		return http.StatusConflict, nil
	}
	if req.Size < 0 || req.Size > directMaxParts*directMaxPartSize {
		return http.StatusBadRequest, fmt.Errorf("unsupported size")
	}
	code, _, body, err := directS3.s3Do(http.MethodPost, t.key, map[string]string{"uploads": ""}, nil, contentTypeFor(t.path))
	if err != nil {
		return http.StatusBadGateway, err
	}
	var res struct {
		UploadID string `xml:"UploadId"`
	}
	if code != http.StatusOK || xml.Unmarshal(body, &res) != nil || res.UploadID == "" {
		logger.Errorf("direct upload create failed: status %d", code)
		return http.StatusBadGateway, fmt.Errorf("could not start upload")
	}
	partSize, partCount := directPartSize(req.Size)
	return directJSON(w, map[string]any{"uploadId": res.UploadID, "partSize": partSize, "partCount": partCount})
}

func directUploadPartsHandler(w http.ResponseWriter, r *http.Request, d *Context) (int, error) {
	req, t, status, err := directResolve(r, d)
	if err != nil {
		return status, err
	}
	if req.UploadID == "" || len(req.PartNumbers) == 0 || len(req.PartNumbers) > 100 {
		return http.StatusBadRequest, fmt.Errorf("need uploadId and 1 to 100 part numbers")
	}
	urls := make(map[string]string, len(req.PartNumbers))
	for _, n := range req.PartNumbers {
		if n < 1 || n > directMaxParts {
			return http.StatusBadRequest, fmt.Errorf("bad part number")
		}
		urls[strconv.Itoa(n)] = directS3.presign(http.MethodPut, t.key, directS3.ttl,
			map[string]string{"partNumber": strconv.Itoa(n), "uploadId": req.UploadID})
	}
	return directJSON(w, map[string]any{"urls": urls})
}

func directUploadCompleteHandler(w http.ResponseWriter, r *http.Request, d *Context) (int, error) {
	req, t, status, err := directResolve(r, d)
	if err != nil {
		return status, err
	}
	if req.UploadID == "" || len(req.Parts) == 0 || len(req.Parts) > directMaxParts {
		return http.StatusBadRequest, fmt.Errorf("need uploadId and parts")
	}
	sort.Slice(req.Parts, func(i, j int) bool { return req.Parts[i].PartNumber < req.Parts[j].PartNumber })
	var b strings.Builder
	b.WriteString("<CompleteMultipartUpload>")
	for _, p := range req.Parts {
		etag := strings.Trim(p.ETag, `"`)
		fmt.Fprintf(&b, "<Part><PartNumber>%d</PartNumber><ETag>\"%s\"</ETag></Part>", p.PartNumber, xmlEscape(etag))
	}
	b.WriteString("</CompleteMultipartUpload>")
	code, _, body, err := directS3.s3Do(http.MethodPost, t.key, map[string]string{"uploadId": req.UploadID}, []byte(b.String()), "application/xml")
	if err != nil {
		return http.StatusBadGateway, err
	}
	// S3 may answer 200 with an <Error> body.
	if code != http.StatusOK || bytes.Contains(body, []byte("<Error>")) {
		logger.Errorf("direct upload complete failed: status %d: %s", code, firstLine(body))
		return http.StatusBadGateway, fmt.Errorf("could not complete upload")
	}
	code, hdr, _, err := directS3.s3Do(http.MethodHead, t.key, nil, nil, "")
	if err != nil || code != http.StatusOK {
		return http.StatusBadGateway, fmt.Errorf("uploaded object not found")
	}
	if got, _ := strconv.ParseInt(hdr.Get("Content-Length"), 10, 64); got != req.Size {
		return http.StatusBadGateway, fmt.Errorf("uploaded size %d does not match %d", got, req.Size)
	}
	directMakeVisible(t)
	activity.RecordUpload(r, toActor(d), t.source, t.path, false)
	return directJSON(w, map[string]any{"ok": true})
}

func directUploadAbortHandler(w http.ResponseWriter, r *http.Request, d *Context) (int, error) {
	req, t, status, err := directResolve(r, d)
	if err != nil {
		return status, err
	}
	if req.UploadID == "" {
		return http.StatusBadRequest, fmt.Errorf("need uploadId")
	}
	code, _, _, err := directS3.s3Do(http.MethodDelete, t.key, map[string]string{"uploadId": req.UploadID}, nil, "")
	if err != nil {
		return http.StatusBadGateway, err
	}
	if code != http.StatusNoContent && code != http.StatusOK && code != http.StatusNotFound {
		return http.StatusBadGateway, fmt.Errorf("abort failed with status %d", code)
	}
	return directJSON(w, map[string]any{"ok": true})
}

// directMakeVisible makes a file written straight to the bucket appear: the
// rclone mount caches directory listings, then the index needs a refresh.
func directMakeVisible(t *directTarget) {
	if rc := directS3.rcloneRC; rc != "" {
		rcloneRC(rc+"/vfs/forget", map[string]string{"file": t.rel})
		// vfs/refresh fails with "file does not exist" for a directory the
		// mount has not seen yet, so refresh every level from the root down:
		// each refresh makes the next level known.
		dirs := []string{""}
		if parent := path.Dir(t.rel); parent != "." && parent != "/" {
			cur := ""
			for _, part := range strings.Split(parent, "/") {
				cur = path.Join(cur, part)
				dirs = append(dirs, cur)
			}
		}
		for _, dir := range dirs {
			args := map[string]string{}
			if dir != "" {
				args["dir"] = dir
			}
			rcloneRC(rc+"/vfs/refresh", args)
		}
	}
	for i := 0; i < 10; i++ { // the mount can lag a moment behind the refresh
		if _, err := os.Stat(t.realPath); err == nil {
			break
		}
		time.Sleep(300 * time.Millisecond)
	}
	if err := files.RefreshIndex(t.source, t.fullIndexPath, false, false); err != nil {
		logger.Debugf("direct upload: refresh index for %s: %v", t.path, err)
	}
	if parent := filepath.Dir(t.fullIndexPath); parent != t.fullIndexPath {
		if err := files.RefreshIndex(t.source, parent, true, false); err != nil {
			logger.Debugf("direct upload: refresh parent of %s: %v", t.path, err)
		}
	}
}

func rcloneRC(endpoint string, args map[string]string) {
	b, _ := json.Marshal(args)
	resp, err := directHTTP.Post(endpoint, "application/json", bytes.NewReader(b))
	if err != nil {
		logger.Errorf("rclone rc %s failed: %v", path.Base(endpoint), err)
		return
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		out, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		logger.Errorf("rclone rc %s: status %d: %s", path.Base(endpoint), resp.StatusCode, firstLine(out))
		return
	}
	// vfs/refresh and vfs/forget answer 200 and report failures per path.
	var res struct {
		Result map[string]string `json:"result"`
	}
	if json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&res) == nil {
		for p, v := range res.Result {
			if v != "OK" {
				logger.Debugf("rclone rc %s %q: %s", path.Base(endpoint), p, v)
			}
		}
	}
}

func xmlEscape(s string) string {
	var b strings.Builder
	_ = xml.EscapeText(&b, []byte(s))
	return b.String()
}

func firstLine(b []byte) string {
	s := string(b)
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		s = s[:i]
	}
	if len(s) > 300 {
		s = s[:300]
	}
	return s
}
