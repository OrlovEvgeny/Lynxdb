import { useEffect } from "preact/hooks";
import { Star, LayoutDashboard, Plus, RefreshCw } from "lucide-preact";
import {
  dashboards,
  dashboardsLoading,
  dashboardsError,
  loadDashboards,
} from "../../stores/dashboards";
import styles from "./DashboardList.module.css";

/** Format a timestamp string as a relative time like "2h ago". */
function timeAgo(iso: string): string {
  const diff = Date.now() - new Date(iso).getTime();
  if (diff < 0) return "just now";
  const secs = Math.floor(diff / 1000);
  if (secs < 60) return "just now";
  const mins = Math.floor(secs / 60);
  if (mins < 60) return `${mins}m ago`;
  const hours = Math.floor(mins / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  if (days < 30) return `${days}d ago`;
  return new Date(iso).toLocaleDateString();
}

export function DashboardList() {
  useEffect(() => {
    loadDashboards();
  }, []);

  const loading = dashboardsLoading.value;
  const error = dashboardsError.value;
  const list = dashboards.value;

  return (
    <div class={styles.page}>
      <div class={styles.header}>
        <h1 class={styles.title}>Dashboards</h1>
        <a href="/dashboards/new" class={styles.newBtn}>
          <Plus size={16} />
          New Dashboard
        </a>
      </div>

      {/* System Overview - always pinned at top */}
      <div class={styles.grid} style={{ marginTop: "var(--space-4)" }}>
        <a href="/dashboards/system" class={`${styles.card} ${styles.systemCard}`}>
          <div class={styles.cardHeader}>
            <Star size={16} class={styles.starIcon} />
            <span class={styles.cardName}>System Overview</span>
          </div>
          <div class={styles.cardMeta}>Built-in server metrics</div>
        </a>
      </div>

      {/* Loading state */}
      {loading && (
        <div class={styles.loading}>
          <RefreshCw size={20} class={styles.spinner} />
          <span>Loading dashboards...</span>
        </div>
      )}

      {/* Error state */}
      {error && !loading && (
        <div class={styles.error}>
          <p>{error}</p>
          <button class={styles.retryBtn} onClick={() => loadDashboards()}>
            Retry
          </button>
        </div>
      )}

      {/* User dashboards */}
      {!loading && !error && list.length > 0 && (
        <div class={styles.grid} style={{ marginTop: "var(--space-4)" }}>
          {list.map((d) => (
            <a key={d.id} href={`/dashboards/${d.id}`} class={styles.card}>
              <div class={styles.cardHeader}>
                <LayoutDashboard size={16} />
                <span class={styles.cardName}>{d.name}</span>
              </div>
              <div class={styles.cardMeta}>
                {d.panels?.length ?? 0} panel{(d.panels?.length ?? 0) !== 1 ? "s" : ""}
                {d.updated_at && <> &middot; {timeAgo(d.updated_at)}</>}
              </div>
            </a>
          ))}
        </div>
      )}

      {/* Empty state */}
      {!loading && !error && list.length === 0 && (
        <div class={styles.empty}>
          <LayoutDashboard size={32} />
          <p>No dashboards yet. Create one to get started.</p>
        </div>
      )}
    </div>
  );
}
