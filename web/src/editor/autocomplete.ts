import {
  autocompletion,
  type CompletionContext,
  type CompletionResult,
  type Completion,
} from "@codemirror/autocomplete";
import {
  fetchFields,
  fetchIndexes,
  fetchFieldValues,
  type FieldInfo,
  type IndexInfo,
} from "../api/client";

// Completion cache -- module-level, refreshed every 60 s or on first trigger

let cachedFields: FieldInfo[] = [];
let cachedIndexes: IndexInfo[] = [];
let lastFetchTime = 0;

const CACHE_TTL_MS = 60_000;

async function ensureCache(): Promise<void> {
  const now = Date.now();
  if (now - lastFetchTime < CACHE_TTL_MS && (cachedFields.length > 0 || cachedIndexes.length > 0)) {
    return;
  }

  // Fetch both in parallel; failures are non-critical -- keep stale cache
  const [fieldsResult, indexesResult] = await Promise.allSettled([
    fetchFields(),
    fetchIndexes(),
  ]);

  if (fieldsResult.status === "fulfilled") {
    cachedFields = fieldsResult.value;
  }
  if (indexesResult.status === "fulfilled") {
    cachedIndexes = indexesResult.value;
  }

  lastFetchTime = now;
}

// Static completion lists

const COMMANDS: readonly string[] = [
  "from", "search", "where", "group", "order", "take", "let", "parse",
  "keep", "omit", "rename", "dedup", "join", "append", "fillnull", "table",
  "top", "bottom", "rare", "sort", "head", "tail", "stats", "eval", "rex",
  "bin", "timechart", "streamstats", "eventstats", "fields", "limit",
  "explode", "pack", "materialize", "every", "bucket", "running", "enrich",
  "rank", "select", "transaction", "xyseries", "multisearch",
];

/** Brief descriptions for commands, shown as detail in autocomplete */
const COMMAND_DOCS: Record<string, string> = {
  from: "Select data source",
  search: "Full-text search",
  where: "Filter rows",
  group: "Group and aggregate",
  order: "Sort results",
  take: "Limit result count",
  let: "Assign expression",
  parse: "Extract fields",
  keep: "Keep specified fields",
  omit: "Remove fields",
  rename: "Rename fields",
  dedup: "Remove duplicates",
  join: "Join datasets",
  append: "Append results",
  fillnull: "Replace nulls",
  table: "Format as table",
  top: "Top N values",
  bottom: "Bottom N values",
  rare: "Least common values",
  sort: "Sort results",
  head: "First N rows",
  tail: "Last N rows",
  stats: "Aggregate statistics",
  eval: "Evaluate expression",
  rex: "Regex field extraction",
  bin: "Bucket values",
  timechart: "Time-series chart",
  streamstats: "Running statistics",
  eventstats: "Event-level stats",
  fields: "Select/remove fields",
  limit: "Limit result count",
  explode: "Expand multivalue",
  pack: "Combine fields",
  materialize: "Create materialized view",
  every: "Recurring schedule",
  bucket: "Bucket values",
  running: "Running aggregate",
  enrich: "Enrich with lookup",
  rank: "Rank rows",
  select: "Select columns",
  transaction: "Group into transactions",
  xyseries: "Pivot to XY series",
  multisearch: "Search multiple sources",
};

const AGG_FUNCTIONS: readonly string[] = [
  "count()", "sum()", "avg()", "min()", "max()", "dc()", "values()",
  "stdev()", "perc50()", "perc75()", "perc90()", "perc95()", "perc99()",
  "earliest()", "latest()",
];

// Per-field value cache so we don't spam the API
const fieldValueCache = new Map<string, { values: Completion[]; fetched: number }>();
const VALUE_CACHE_TTL_MS = 30_000;

// Context detection helpers

/** Return the word fragment currently being typed (if any). */
function currentWord(line: string): { word: string } {
  const match = line.match(/(\w*)$/);
  if (match) {
    return { word: match[1] };
  }
  return { word: "" };
}

// Completion source

async function lynxflowCompletion(
  context: CompletionContext,
): Promise<CompletionResult | null> {
  // Only complete when the user has typed something or explicitly invoked
  const textBefore = context.state.doc.sliceString(0, context.pos);

  // Do not trigger on empty input or pure whitespace unless explicit
  if (!context.explicit && textBefore.trim() === "") return null;

  // Lazy-load the cache (non-blocking; uses stale data if fetch fails)
  await ensureCache();

  const beforeCursor = textBefore;
  const { word } = currentWord(beforeCursor);
  const absFrom = context.pos - word.length;
  const lowerWord = word.toLowerCase();

  // --- After "field=" or "field!=" -> field values ---
  // Match patterns like: level=err, level="err, status!=2
  const fieldValueMatch = beforeCursor.match(/(\w+)[!=]+["']?(\w*)$/);
  if (fieldValueMatch) {
    const fieldName = fieldValueMatch[1];
    // Verify it's a known field
    const isKnownField = cachedFields.some((f) => f.name === fieldName);
    if (isKnownField) {
      const values = await getFieldValues(fieldName);
      if (values.length > 0) {
        return {
          from: context.pos - fieldValueMatch[2].length,
          options: values,
          filter: true,
        };
      }
    }
  }

  // --- After pipe or at very start -> commands ---
  if (/\|\s*\w*$/.test(beforeCursor) || beforeCursor.trim() === word) {
    // Only suggest commands if the only thing typed is the partial word,
    // or we are right after a pipe.
    const isPipe = /\|\s*\w*$/.test(beforeCursor);
    const isStart = beforeCursor.trim() === word;
    if (isPipe || isStart) {
      // Need at least 1 char or explicit trigger to avoid noise
      if (!context.explicit && word.length === 0 && !isPipe) return null;
      return {
        from: absFrom,
        options: COMMANDS.map((cmd) => ({
          label: cmd,
          type: "keyword",
          detail: COMMAND_DOCS[cmd] || "command",
          boost: lowerWord && cmd.toLowerCase().startsWith(lowerWord) ? 1 : 0,
        })),
        filter: true,
      };
    }
  }

  // --- After "from " -> index names ---
  if (/\bfrom\s+\w*$/.test(beforeCursor)) {
    return {
      from: absFrom,
      options: cachedIndexes.map((idx) => ({
        label: idx.name,
        type: "variable",
        detail: "index",
        boost: lowerWord && idx.name.toLowerCase().startsWith(lowerWord) ? 1 : 0,
      })),
      filter: true,
    };
  }

  // --- After "by ", "where ", "group ", "order ", "keep ", "omit " -> field names ---
  if (/\b(?:by|where|group|order|keep|omit|on)\s+\w*$/.test(beforeCursor)) {
    return {
      from: absFrom,
      options: cachedFields.map((f) => ({
        label: f.name,
        type: "property",
        detail: f.type,
        boost: lowerWord && f.name.toLowerCase().startsWith(lowerWord) ? 1 : 0,
      })),
      filter: true,
    };
  }

  // --- After comma in a field list (by field1, field2) -> field names ---
  if (/\b(?:by|keep|omit)\s+[\w,\s]+,\s*\w*$/.test(beforeCursor)) {
    return {
      from: absFrom,
      options: cachedFields.map((f) => ({
        label: f.name,
        type: "property",
        detail: f.type,
        boost: lowerWord && f.name.toLowerCase().startsWith(lowerWord) ? 1 : 0,
      })),
      filter: true,
    };
  }

  // --- After "compute " or "stats " -> aggregation functions ---
  if (/\b(?:compute|stats)\s+\w*$/.test(beforeCursor)) {
    return {
      from: absFrom,
      options: AGG_FUNCTIONS.map((fn) => ({
        label: fn,
        type: "function",
        detail: "function",
        apply: fn,
        boost: lowerWord && fn.toLowerCase().startsWith(lowerWord) ? 1 : 0,
      })),
      filter: true,
    };
  }

  // --- After comma in compute/stats list -> aggregation functions ---
  if (/\b(?:compute|stats)\s+[\w(),\s]+,\s*\w*$/.test(beforeCursor)) {
    return {
      from: absFrom,
      options: AGG_FUNCTIONS.map((fn) => ({
        label: fn,
        type: "function",
        detail: "function",
        apply: fn,
        boost: lowerWord && fn.toLowerCase().startsWith(lowerWord) ? 1 : 0,
      })),
      filter: true,
    };
  }

  // --- Fallback: if user typed 2+ chars, try field names ---
  if (word.length >= 2) {
    return {
      from: absFrom,
      options: cachedFields.map((f) => ({
        label: f.name,
        type: "property",
        detail: f.type,
        boost: lowerWord && f.name.toLowerCase().startsWith(lowerWord) ? 1 : 0,
      })),
      filter: true,
    };
  }

  return null;
}

// Field value fetching with cache

async function getFieldValues(fieldName: string): Promise<Completion[]> {
  const cached = fieldValueCache.get(fieldName);
  if (cached && Date.now() - cached.fetched < VALUE_CACHE_TTL_MS) {
    return cached.values;
  }

  try {
    const raw = await fetchFieldValues(fieldName, 20);
    const completions: Completion[] = raw.map((v) => ({
      label: v.value,
      type: "text",
      detail: `${v.count}`,
    }));
    fieldValueCache.set(fieldName, { values: completions, fetched: Date.now() });
    return completions;
  } catch {
    return cached?.values ?? [];
  }
}

// Public API -- returns a CodeMirror extension

export function lynxflowAutocompletion() {
  return autocompletion({
    override: [lynxflowCompletion],
    activateOnTyping: true,
    // Override the default "accept with Enter" so Enter still runs the query.
    // Users can accept completions with Tab instead.
    defaultKeymap: true,
  });
}
