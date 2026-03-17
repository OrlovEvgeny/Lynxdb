import { useState } from "preact/hooks";
import { PipelineFlow } from "./PipelineFlow";
import type { ExplainResult, QueryStats } from "../api/client";
import type { DetailedStats } from "../api/client";
import { formatMs } from "../utils/format";
import styles from "./ExplainInspector.module.css";

interface ExplainInspectorProps {
  explain: ExplainResult;
  stats?: QueryStats | null;
}

type TabId = "pipeline" | "optimizer" | "scan" | "timing";

/** Format a rule name: replace underscores with spaces, title case. */
function formatRuleName(name: string): string {
  return name
    .replace(/_/g, " ")
    .replace(/\b\w/g, (c) => c.toUpperCase());
}

export function ExplainInspector({ explain, stats }: ExplainInspectorProps) {
  const [activeTab, setActiveTab] = useState<TabId>("pipeline");

  const parsed = explain.parsed;
  if (!parsed) return null;

  const ds = stats?.stats as DetailedStats | undefined;

  const tabs: { id: TabId; label: string }[] = [
    { id: "pipeline", label: "Pipeline" },
    { id: "optimizer", label: "Optimizer Rules" },
    { id: "scan", label: "Scan Plan" },
    { id: "timing", label: "Timing" },
  ];

  return (
    <div class={styles.inspector}>
      <div class={styles.tabBar}>
        {tabs.map((tab) => (
          <button
            key={tab.id}
            type="button"
            class={`${styles.tab}${activeTab === tab.id ? ` ${styles.tabActive}` : ""}`}
            onClick={() => setActiveTab(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </div>

      <div class={styles.tabContent}>
        {activeTab === "pipeline" && (
          <PipelineFlow
            stages={parsed.pipeline}
            optimizerRules={parsed.optimizer_rules}
          />
        )}

        {activeTab === "optimizer" && (
          <OptimizerRulesTab rules={parsed.optimizer_rules} />
        )}

        {activeTab === "scan" && (
          <ScanPlanTab parsed={parsed} />
        )}

        {activeTab === "timing" && (
          <TimingTab parsed={parsed} ds={ds} />
        )}
      </div>
    </div>
  );
}

// ---------------------------------------------------------------------------
// Optimizer Rules tab
// ---------------------------------------------------------------------------

function OptimizerRulesTab({ rules }: { rules?: { name: string; description?: string; count: number }[] }) {
  if (!rules || rules.length === 0) {
    return <div class={styles.emptyState}>No optimizer rules applied</div>;
  }

  return (
    <div>
      {rules.map((rule) => (
        <div key={rule.name} class={styles.ruleItem}>
          <span class={styles.ruleName}>{formatRuleName(rule.name)}</span>
          {rule.description && (
            <span class={styles.ruleDesc}>{rule.description}</span>
          )}
          {rule.count > 1 && (
            <span class={styles.ruleCount}>x{rule.count}</span>
          )}
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Scan Plan tab
// ---------------------------------------------------------------------------

function ScanPlanTab({ parsed }: { parsed: NonNullable<ExplainResult["parsed"]> }) {
  const rows: { label: string; value: string }[] = [];

  if (parsed.source_scope) {
    rows.push({ label: "Source scope", value: parsed.source_scope.type });
    if (parsed.source_scope.resolved_sources?.length) {
      rows.push({
        label: "Resolved sources",
        value: parsed.source_scope.resolved_sources.join(", "),
      });
    }
  }

  rows.push({
    label: "Search terms",
    value: parsed.search_terms?.length ? parsed.search_terms.join(", ") : "none",
  });

  rows.push({
    label: "Time bounds",
    value: parsed.has_time_bounds ? "Yes" : "No",
  });

  rows.push({
    label: "Full scan",
    value: parsed.uses_full_scan ? "Yes" : "No",
  });

  rows.push({
    label: "Fields read",
    value: parsed.fields_read?.length ? parsed.fields_read.join(", ") : "all",
  });

  if (parsed.physical_plan) {
    const pp = parsed.physical_plan;
    const flags: string[] = [];
    if (pp.count_star_only) flags.push("count(*) optimized");
    if (pp.partial_agg) flags.push("partial aggregation");
    if (pp.topk_agg) flags.push(`TopK (k=${pp.topk ?? "?"})`);
    if (pp.join_strategy) flags.push(`join: ${pp.join_strategy}`);
    if (flags.length > 0) {
      rows.push({ label: "Physical plan", value: flags.join(", ") });
    }
  }

  return (
    <div>
      {rows.map((row) => (
        <div key={row.label} class={styles.scanRow}>
          <span class={styles.scanLabel}>{row.label}</span>
          <span class={styles.scanValue}>{row.value}</span>
        </div>
      ))}
    </div>
  );
}

// ---------------------------------------------------------------------------
// Timing tab
// ---------------------------------------------------------------------------

function TimingTab({
  parsed,
  ds,
}: {
  parsed: NonNullable<ExplainResult["parsed"]>;
  ds?: DetailedStats;
}) {
  const entries: { label: string; ms: number }[] = [];

  if (parsed.parse_ms != null) entries.push({ label: "Parse", ms: parsed.parse_ms });
  if (parsed.optimize_ms != null) entries.push({ label: "Optimize", ms: parsed.optimize_ms });
  if (ds?.scan_ms != null) entries.push({ label: "Scan", ms: ds.scan_ms });
  if (ds?.pipeline_ms != null) entries.push({ label: "Pipeline", ms: ds.pipeline_ms });

  if (entries.length === 0) {
    return <div class={styles.emptyState}>Timing data not available</div>;
  }

  const maxMs = Math.max(...entries.map((e) => e.ms), 1);

  return (
    <div>
      {entries.map((entry) => {
        const widthPercent = Math.max((entry.ms / maxMs) * 100, 2);
        return (
          <div key={entry.label} class={styles.timingRow}>
            <span class={styles.timingLabel}>{entry.label}</span>
            <div
              class={styles.timingBar}
              style={{ width: `${widthPercent}%` }}
            />
            <span class={styles.timingValue}>{formatMs(entry.ms)}</span>
          </div>
        );
      })}
    </div>
  );
}
