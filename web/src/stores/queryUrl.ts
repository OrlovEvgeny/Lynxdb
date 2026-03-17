/**
 * URL hash encoding/decoding for query sharing.
 * Format: #q=level%3Derror&from=-1h&to=...
 */

/**
 * Write the current query and time range into the URL hash.
 * Uses replaceState to avoid polluting browser history.
 * Optionally includes page and size for pagination state.
 */
export function writeQueryToHash(
  query: string,
  fromRange: string,
  toRange?: string,
  page?: number,
  size?: number,
): void {
  const params = new URLSearchParams();
  params.set("q", query);
  params.set("from", fromRange);
  if (toRange) params.set("to", toRange);
  if (page !== undefined && page > 1) params.set("page", String(page));
  if (size !== undefined && size !== 100) params.set("size", String(size));
  window.history.replaceState(null, "", "#" + params.toString());
}

/**
 * Read query and time range from the URL hash.
 * Returns null if hash is empty or has no `q` param.
 * Also parses optional page and size pagination params.
 */
export function readQueryFromHash(): {
  q: string;
  from?: string;
  to?: string;
  page?: number;
  size?: number;
} | null {
  const hash = window.location.hash.slice(1);
  if (!hash) return null;
  const params = new URLSearchParams(hash);
  const q = params.get("q");
  if (!q) return null;

  const pageStr = params.get("page");
  const sizeStr = params.get("size");

  return {
    q,
    from: params.get("from") || undefined,
    to: params.get("to") || undefined,
    page: pageStr ? Number(pageStr) : undefined,
    size: sizeStr ? Number(sizeStr) : undefined,
  };
}
