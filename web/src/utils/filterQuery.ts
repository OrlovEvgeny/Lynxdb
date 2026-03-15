/**
 * Shared utility for building filter clauses that are appended to the
 * current SPL2 query text.  Used by EventDetail field filter buttons and
 * (in the future) field sidebar value drill-down popovers.
 */

/**
 * Append a `| where field="value"` (or `!=`) clause to the current query.
 *
 * @param currentQuery - The existing query text (may be empty).
 * @param field        - The field name to filter on.
 * @param value        - The field value to match.
 * @param exclude      - When true, uses `!=` instead of `=`.
 * @returns The modified query string.
 */
export function appendFilter(
  currentQuery: string,
  field: string,
  value: string,
  exclude: boolean,
): string {
  if (value == null) return currentQuery;

  const escaped = value.replace(/"/g, '\\"');
  const op = exclude ? "!=" : "=";
  const clause = `where ${field}${op}"${escaped}"`;

  const trimmed = currentQuery.trim();
  if (!trimmed) {
    return `| ${clause}`;
  }

  return `${trimmed} | ${clause}`;
}
