import { useState, useEffect, useCallback } from "preact/hooks";
import { fetchStatus } from "../../api/client";
import type { DashboardPanel } from "../../api/client";
import { formatCount, formatBytes, formatUptime } from "../../utils/format";
import { DashboardHeader } from "./DashboardHeader";
import { PanelChrome } from "./PanelChrome";
import { PanelRenderer } from "./PanelRenderer";

// ---------------------------------------------------------------------------
// System overview panel definitions
// ---------------------------------------------------------------------------

interface SystemPanel extends DashboardPanel {
  dataSource?: "stats";
  statsField?: string;
  format?: "bytes" | "count" | "uptime";
}

const SYSTEM_OVERVIEW_PANELS: SystemPanel[] = [
  {
    id: "sys-storage",
    title: "Storage Used",
    type: "stat",
    q: "",
    dataSource: "stats",
    statsField: "storage.used_bytes",
    format: "bytes",
    position: { x: 0, y: 0, w: 3, h: 3 },
  },
  {
    id: "sys-events",
    title: "Total Events",
    type: "stat",
    q: "",
    dataSource: "stats",
    statsField: "events.total",
    format: "count",
    position: { x: 3, y: 0, w: 3, h: 3 },
  },
  {
    id: "sys-uptime",
    title: "Uptime",
    type: "stat",
    q: "",
    dataSource: "stats",
    statsField: "uptime_seconds",
    format: "uptime",
    position: { x: 6, y: 0, w: 3, h: 3 },
  },
  {
    id: "sys-errorrate",
    title: "Error Rate",
    type: "timechart",
    q: "level=error | timechart count span=5m",
    position: { x: 9, y: 0, w: 3, h: 3 },
  },
  {
    id: "sys-bylevel",
    title: "Events by Level",
    type: "bar",
    q: "* | stats count by level",
    position: { x: 0, y: 3, w: 4, h: 4 },
  },
  {
    id: "sys-topsrc",
    title: "Top Sources",
    type: "bar",
    q: "* | stats count by source | sort -count | head 10",
    position: { x: 4, y: 3, w: 4, h: 4 },
  },
  {
    id: "sys-eventrate",
    title: "Event Rate",
    type: "timechart",
    q: "* | timechart count span=5m",
    position: { x: 8, y: 3, w: 4, h: 4 },
  },
  {
    id: "sys-errors",
    title: "Recent Errors",
    type: "table",
    q: "level=error | head 20 | table _time, source, _raw",
    position: { x: 0, y: 7, w: 12, h: 5 },
  },
];

// ---------------------------------------------------------------------------
// Stats-based stat panel hook
// ---------------------------------------------------------------------------

function useStatsPanel(
  statsField: string,
  fmt: "bytes" | "count" | "uptime",
  refreshTick: number,
): { display: string; loading: boolean; error: string | null; refresh: () => void } {
  const [display, setDisplay] = useState("--");
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  const execute = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await fetchStatus();
      // Navigate dotted path (e.g., "storage.used_bytes")
      const parts = statsField.split(".");
      let val: unknown = data;
      for (const part of parts) {
        if (val && typeof val === "object" && !Array.isArray(val)) {
          val = (val as Record<string, unknown>)[part];
        } else {
          val = undefined;
          break;
        }
      }
      const num = typeof val === "number" ? val : 0;
      switch (fmt) {
        case "bytes":
          setDisplay(formatBytes(num));
          break;
        case "uptime":
          setDisplay(formatUptime(num));
          break;
        default:
          setDisplay(formatCount(num));
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to fetch stats");
    } finally {
      setLoading(false);
    }
  }, [statsField, fmt]);

  useEffect(() => {
    execute();
  }, [execute, refreshTick]);

  return { display, loading, error, refresh: execute };
}

// ---------------------------------------------------------------------------
// Stat panel that uses /api/v1/stats
// ---------------------------------------------------------------------------

function StatsStatPanel({
  panel,
  refreshTick,
}: {
  panel: SystemPanel;
  refreshTick: number;
}) {
  const { display, loading, error, refresh } = useStatsPanel(
    panel.statsField ?? "",
    panel.format ?? "count",
    refreshTick,
  );

  return (
    <PanelChrome
      title={panel.title}
      loading={loading}
      error={error}
      onRefresh={refresh}
    >
      <div
        style={{
          display: "flex",
          flexDirection: "column",
          alignItems: "center",
          justifyContent: "center",
          height: "100%",
          gap: "4px",
        }}
      >
        <div
          style={{
            fontSize: "2.5rem",
            fontWeight: "700",
            color: "var(--text-primary)",
            lineHeight: "1.1",
          }}
        >
          {display}
        </div>
        <div
          style={{
            fontSize: "0.78rem",
            color: "var(--text-secondary)",
          }}
        >
          {panel.title}
        </div>
      </div>
    </PanelChrome>
  );
}

// ---------------------------------------------------------------------------
// System Overview Component
// ---------------------------------------------------------------------------

export function SystemOverview() {
  const [from, setFrom] = useState(() => localStorage.getItem("lynxdb-dash-timerange") || "-1h");
  const [refreshInterval, setRefreshInterval] = useState(30000);
  const [refreshTick, setRefreshTick] = useState(0);
  const variables: Record<string, string> = {};

  // Auto-refresh
  useEffect(() => {
    if (refreshInterval <= 0) return;
    const id = setInterval(() => {
      setRefreshTick((t) => t + 1);
    }, refreshInterval);
    return () => clearInterval(id);
  }, [refreshInterval]);

  const handleTimeChange = useCallback((newFrom: string) => {
    setFrom(newFrom);
    localStorage.setItem("lynxdb-dash-timerange", newFrom);
    setRefreshTick((t) => t + 1);
  }, []);

  return (
    <div style={{ display: "flex", flexDirection: "column", height: "100%" }}>
      <DashboardHeader
        name="System Overview"
        editable={false}
        from={from}
        onTimeChange={handleTimeChange}
        refreshInterval={refreshInterval}
        onRefreshIntervalChange={setRefreshInterval}
      />
      <div
        style={{
          flex: 1,
          overflowY: "auto",
          padding: "var(--space-4)",
        }}
      >
        <div
          style={{
            display: "grid",
            gridTemplateColumns: "repeat(12, 1fr)",
            gap: "8px",
          }}
        >
          {SYSTEM_OVERVIEW_PANELS.map((panel) => {
            const gridStyle = {
              gridColumn: `${panel.position.x + 1} / span ${panel.position.w}`,
              gridRow: `${panel.position.y + 1} / span ${panel.position.h}`,
              minHeight: `${panel.position.h * 80}px`,
            };

            if (panel.dataSource === "stats") {
              return (
                <div key={panel.id} style={gridStyle}>
                  <StatsStatPanel panel={panel} refreshTick={refreshTick} />
                </div>
              );
            }

            return (
              <div key={panel.id} style={gridStyle}>
                <PanelRenderer
                  panel={panel}
                  from={from}
                  variables={variables}
                  refreshTick={refreshTick}
                />
              </div>
            );
          })}
        </div>
      </div>
    </div>
  );
}
