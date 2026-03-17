import { signal } from "@preact/signals";
import { fetchDashboards, type DashboardSummary } from "../api/client";

/** List of user dashboards. */
export const dashboards = signal<DashboardSummary[]>([]);

/** Whether the dashboards list is currently loading. */
export const dashboardsLoading = signal<boolean>(false);

/** Error message from the last load attempt, or null. */
export const dashboardsError = signal<string | null>(null);

/** Fetch all dashboards from the API and update signals. */
export async function loadDashboards(): Promise<void> {
  dashboardsLoading.value = true;
  dashboardsError.value = null;
  try {
    dashboards.value = await fetchDashboards();
  } catch (err) {
    dashboardsError.value =
      err instanceof Error ? err.message : "Failed to load dashboards";
  } finally {
    dashboardsLoading.value = false;
  }
}
