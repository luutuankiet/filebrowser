// Direct S3 uploads (opt-in on the server): the browser PUTs the file's parts
// straight to the bucket on presigned URLs, so the bytes never pass through
// this server. The server only starts, presigns, completes or aborts the
// multipart upload. When the server has the feature off it answers 404 and
// the caller falls back to the normal upload path.

import { fetchURL } from "@/api/utils";
import { getApiPath } from "@/utils/url";
import { getters } from "@/store/getters";

const PARALLEL_PARTS = 4;
const PRESIGN_BATCH = 50;
const MAX_TRIES = 5;

let directOff = false;

async function call(method, path, body) {
  const res = await fetchURL(getApiPath(path), {
    method,
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  return res.status === 204 ? {} : res.json();
}

function httpError(status, message) {
  const err = new Error(message || `HTTP ${status}`);
  err.response = { status };
  return err;
}

function putPart(url, blob, onProgress, xhrs) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhrs.add(xhr);
    xhr.open("PUT", url);
    xhr.upload.onprogress = (e) => onProgress(e.loaded);
    xhr.onload = () => {
      xhrs.delete(xhr);
      if (xhr.status >= 200 && xhr.status < 300) {
        const etag = xhr.getResponseHeader("ETag");
        if (!etag) {
          reject(new Error("No ETag on part response: the bucket CORS rule must expose ETag"));
          return;
        }
        resolve(etag);
      } else {
        reject(Object.assign(new Error(`Part upload failed: HTTP ${xhr.status}`), { status: xhr.status }));
      }
    };
    xhr.onerror = () => {
      xhrs.delete(xhr);
      reject(new Error("Network error"));
    };
    xhr.onabort = () => {
      xhrs.delete(xhr);
      reject(new Error("Upload aborted"));
    };
    xhr.send(blob);
  });
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/**
 * Upload one file straight to the bucket.
 * Returns false when direct transfers do not apply (caller uses the normal path),
 * true when the file is uploaded, and throws on failure ("Upload aborted" when
 * paused or cancelled). State lives on upload.direct so resume continues.
 */
export async function tryDirectUpload(upload, manager) {
  if (directOff || getters.isShare() || upload.type === "directory" || !upload.size) {
    return false;
  }
  const target = { source: upload.source, path: upload.path };

  if (!upload.direct) {
    let created;
    try {
      created = await call("POST", "transfers/uploads", {
        ...target,
        size: upload.size,
        overwrite: !!upload.overwrite,
      });
    } catch (err) {
      if (err.status === 404) {
        directOff = true; // off on this server, or path not on the bucket mount
        return false;
      }
      throw httpError(err.status, err.message);
    }
    upload.direct = { ...created, etags: {} };
  }
  const st = upload.direct;

  let aborted = false;
  const xhrs = new Set();
  upload.xhrPromise = {
    abort() {
      aborted = true;
      xhrs.forEach((x) => x.abort());
      // cancel() removes the upload from the queue right after aborting; pause() keeps it.
      setTimeout(() => {
        if (!manager.findById(upload.id) && upload.direct) {
          const { uploadId } = upload.direct;
          upload.direct = null;
          call("DELETE", "transfers/uploads", { ...target, uploadId }).catch((e) =>
            console.warn("direct upload abort failed", e)
          );
        }
      }, 0);
    },
  };

  const inFlight = {};
  const doneBytes = () => {
    let n = 0;
    for (const p of Object.keys(st.etags)) n += partLength(Number(p));
    for (const v of Object.values(inFlight)) n += v;
    return n;
  };
  const partLength = (n) => Math.min(st.partSize, upload.size - (n - 1) * st.partSize);
  const report = () =>
    manager.updateProgress(upload, Math.round((doneBytes() / upload.size) * 1000) / 10);

  const pending = [];
  for (let n = 1; n <= st.partCount; n++) if (!st.etags[n]) pending.push(n);

  const urls = {};
  const presign = async (n) => {
    if (urls[n]) return urls[n];
    const batch = pending.filter((p) => p >= n && !urls[p]).slice(0, PRESIGN_BATCH);
    const res = await call("POST", "transfers/uploads/parts", {
      ...target,
      uploadId: st.uploadId,
      partNumbers: batch,
    });
    Object.assign(urls, res.urls);
    return urls[n];
  };

  let next = 0;
  const worker = async () => {
    while (!aborted && next < pending.length) {
      const n = pending[next++];
      const start = (n - 1) * st.partSize;
      const blob = upload.file.slice(start, start + partLength(n));
      for (let attempt = 1; ; attempt++) {
        if (aborted) throw new Error("Upload aborted");
        try {
          const url = await presign(n);
          st.etags[n] = await putPart(url, blob, (loaded) => {
            inFlight[n] = loaded;
            report();
          }, xhrs);
          delete inFlight[n];
          report();
          break;
        } catch (err) {
          delete inFlight[n];
          if (aborted || err.message === "Upload aborted") throw new Error("Upload aborted");
          if (err.status === 403) delete urls[n]; // expired URL: presign again
          if (attempt >= MAX_TRIES) throw err;
          await sleep(1000 * 2 ** (attempt - 1));
        }
      }
    }
  };

  manager.startProgressTimeout(upload);
  try {
    await Promise.all(Array.from({ length: Math.min(PARALLEL_PARTS, pending.length) }, worker));
  } catch (err) {
    // Stop the sibling parts; keep upload.direct so a retry resumes.
    const wasAborted = aborted;
    aborted = true;
    xhrs.forEach((x) => x.abort());
    throw wasAborted ? new Error("Upload aborted") : err;
  }
  if (aborted) throw new Error("Upload aborted");

  const parts = Object.entries(st.etags).map(([n, etag]) => ({ partNumber: Number(n), etag }));
  await call("POST", "transfers/uploads/complete", {
    ...target,
    uploadId: st.uploadId,
    size: upload.size,
    parts,
  });
  upload.direct = null;
  return true;
}
