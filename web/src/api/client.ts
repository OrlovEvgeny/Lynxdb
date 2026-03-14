import { authHeaders, handleAuthError } from "./auth";

const BASE = "";

/** All API responses are wrapped in {data: T, meta?: {...}} */
interface APIResponse<T> {
  data: T;
  meta?: {
    took_ms?: number;
    scanned?: number;
    query_id?: string;
    stats?: Record<string, unknown>;
  };
}

/** Wrapper around fetch that injects auth headers and handles 401. */
async function apiFetch(input: string, init?: RequestInit): Promise<Response> {
  const headers = { ...authHeaders(), ...init?.headers };
  const resp = await fetch(input, { ...init, headers });
  handleAuthError(resp);
  return resp;
}

/** Events query result */
export interface EventsResult {
  type: "events";
  events: Record<string, unknown>[];
  total: number;
  has_more: boolean;
}

/** Aggregate query result */
export interface AggregateResult {
  type: "aggregate" | "timechart";
  columns: string[];
  rows: unknown[][];
  total_rows: number;
}

export type QueryResult = EventsResult | AggregateResult;

/** Query execution stats from meta envelope */
export interface QueryStats {
  took_ms: number;
  scanned: number;
  query_id?: string;
  stats?: {
    rows_scanned?: number;
    rows_returned?: number;
    matched_rows?: number;
    segments_total?: number;
    segments_scanned?: number;
    [key: string]: unknown;
  };
}

export interface QueryResponse {
  result: QueryResult;
  stats: QueryStats;
}

export async function executeQuery(
  query: string,
  from?: string,
  to?: string,
  limit?: number,
): Promise<QueryResponse> {
  const body: Record<string, unknown> = { q: query };
  if (from) body.from = from;
  if (to) body.to = to;
  if (limit) body.limit = limit;

  const resp = await apiFetch(`${BASE}/api/v1/query`, {
    method: "POST",
    body: JSON.stringify(body),
  });

  if (!resp.ok) {
    const err = await resp
      .json()
      .catch(() => ({ error: { message: resp.statusText } }));
    throw new Error(
      err.error?.message || err.data?.error || resp.statusText,
    );
  }

  const json = await resp.json();
  return {
    result: json.data as QueryResult,
    stats: {
      took_ms: json.meta?.took_ms ?? 0,
      scanned: json.meta?.scanned ?? 0,
      query_id: json.meta?.query_id,
      stats: json.meta?.stats,
    },
  };
}

export interface FieldInfo {
  name: string;
  type: string;
  count: number;
  coverage: number;
}

export async function fetchFields(): Promise<FieldInfo[]> {
  const resp = await apiFetch(`${BASE}/api/v1/fields`);
  if (!resp.ok) throw new Error("Failed to fetch fields");
  const json: APIResponse<{ fields: FieldInfo[] }> = await resp.json();
  return json.data.fields;
}

export interface FieldValue {
  value: string;
  count: number;
}

export async function fetchFieldValues(
  name: string,
  limit = 10,
): Promise<FieldValue[]> {
  const resp = await apiFetch(
    `${BASE}/api/v1/fields/${encodeURIComponent(name)}/values?limit=${limit}`,
  );
  if (!resp.ok) throw new Error("Failed to fetch field values");
  const json = await resp.json();
  return json.data.values ?? json.data ?? [];
}

export interface IndexInfo {
  name: string;
  retention_period: string;
  replication_factor: number;
}

export async function fetchIndexes(): Promise<IndexInfo[]> {
  const resp = await apiFetch(`${BASE}/api/v1/indexes`);
  if (!resp.ok) throw new Error("Failed to fetch indexes");
  const json: APIResponse<{ indexes: IndexInfo[] }> = await resp.json();
  return json.data.indexes;
}

export interface HistogramBucket {
  time: string;
  count: number;
}

export interface HistogramResult {
  interval: string;
  buckets: HistogramBucket[];
  total: number;
}

export async function fetchHistogram(
  from?: string,
  to?: string,
  buckets = 50,
  index?: string,
): Promise<HistogramResult> {
  const params = new URLSearchParams();
  if (from) params.set("from", from);
  if (to) params.set("to", to);
  params.set("buckets", String(buckets));
  if (index) params.set("index", index);

  const resp = await apiFetch(`${BASE}/api/v1/histogram?${params}`);
  if (!resp.ok) throw new Error("Failed to fetch histogram");
  const json = await resp.json();
  return json.data;
}

export async function fetchStatus(): Promise<Record<string, unknown>> {
  const resp = await apiFetch(`${BASE}/api/v1/status`);
  if (!resp.ok) throw new Error("Failed to fetch status");
  const json = await resp.json();
  return json.data;
}
