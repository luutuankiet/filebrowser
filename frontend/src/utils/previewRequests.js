/**
 * Fetch a preview image and return an object URL for display.
 * Pass an AbortSignal so the caller can cancel (e.g. on navigation or unmount).
 * @param {string} url - Preview API URL from getPreviewURL / getPreviewURLPublic
 * @param {AbortSignal} signal
 * @returns {Promise<string>} Blob object URL — caller must revoke when done
 */
export async function fetchPreviewImage(url, signal) {
  const res = await fetch(url, {
    credentials: "same-origin",
    signal,
    // Fork: no custom header. The preview endpoint does not read it, and the
    // server may redirect to a bucket on another origin, where a custom
    // header forces a CORS preflight per thumbnail.
  });

  if (res.status < 200 || res.status > 299) {
    throw new Error(`Preview request failed: ${res.status}`);
  }

  const blob = await res.blob();
  return URL.createObjectURL(blob);
}
