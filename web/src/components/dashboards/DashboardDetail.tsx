import { useState, useEffect, useRef, useCallback } from "preact/hooks";
import { route } from "preact-router";
import { GridStack } from "gridstack";
import "gridstack/dist/gridstack.min.css";
import type { DashboardPanel, DashboardSummary, DashboardVariable } from "../../api/client";
import {
  fetchDashboard,
  createDashboard,
  updateDashboard,
  deleteDashboard,
} from "../../api/client";
import { DashboardHeader } from "./DashboardHeader";
import { PanelRenderer } from "./PanelRenderer";
import { PanelEditForm } from "./PanelEditForm";
import { VariableBar } from "./VariableBar";
import { VariableEditor } from "./VariableEditor";
import styles from "./DashboardDetail.module.css";

interface DashboardDetailProps {
  dashboardId: string | null;
  editMode?: boolean;
}

export function DashboardDetail({
  dashboardId,
  editMode: initialEditMode,
}: DashboardDetailProps) {
  const [dashboard, setDashboard] = useState<DashboardSummary | null>(null);
  const [loading, setLoading] = useState(!!dashboardId);
  const [error, setError] = useState<string | null>(null);
  const [editMode, setEditMode] = useState(initialEditMode ?? false);
  const [from, setFrom] = useState(() => localStorage.getItem("lynxdb-dash-timerange") || "-1h");
  const [refreshInterval, setRefreshInterval] = useState(0);
  const [refreshTick, setRefreshTick] = useState(0);
  const [dashName, setDashName] = useState("");
  const [unsavedPanels, setUnsavedPanels] = useState<DashboardPanel[]>([]);
  const [unsavedVariables, setUnsavedVariables] = useState<DashboardVariable[]>([]);
  const [variableValues, setVariableValues] = useState<Record<string, string>>({});
  const [showVariableEditor, setShowVariableEditor] = useState(false);
  const [showPanelForm, setShowPanelForm] = useState(false);
  const [editingPanel, setEditingPanel] = useState<DashboardPanel | null>(null);

  const gridRef = useRef<HTMLDivElement>(null);
  const gsRef = useRef<GridStack | null>(null);
  const registeredPanels = useRef<Set<string>>(new Set());

  // Fetch dashboard
  useEffect(() => {
    if (!dashboardId) {
      setDashName("Untitled Dashboard");
      setUnsavedPanels([]);
      setEditMode(true);
      return;
    }
    let cancelled = false;
    setLoading(true);
    setError(null);
    fetchDashboard(dashboardId)
      .then((d) => {
        if (cancelled) return;
        setDashboard(d);
        setDashName(d.name);
        setUnsavedPanels(d.panels);
        setUnsavedVariables(d.variables ?? []);
        // Initialize variable values from defaults
        const initVals: Record<string, string> = {};
        for (const v of d.variables ?? []) {
          initVals[v.name] = v.default ?? "*";
        }
        setVariableValues(initVals);
      })
      .catch((err) => {
        if (cancelled) return;
        setError(err instanceof Error ? err.message : "Failed to load dashboard");
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, [dashboardId]);

  // Initialize GridStack once, re-init on editMode toggle
  useEffect(() => {
    const el = gridRef.current;
    if (!el) return;

    if (gsRef.current) {
      gsRef.current.destroy(false);
      gsRef.current = null;
    }

    const gs = GridStack.init(
      {
        column: 12,
        cellHeight: 80,
        margin: 8,
        disableResize: !editMode,
        disableDrag: !editMode,
        float: true,
        animate: true,
      },
      el,
    );
    gsRef.current = gs;
    registeredPanels.current = new Set();

    // Register any existing Preact-rendered items
    const existingItems = el.querySelectorAll<HTMLElement>(".grid-stack-item");
    for (const item of existingItems) {
      const id = item.getAttribute("gs-id");
      if (id) {
        gs.makeWidget(item);
        registeredPanels.current.add(id);
      }
    }

    // Listen for layout changes in edit mode
    gs.on("change", () => {
      if (!editMode) return;
      const items = gs.getGridItems();
      setUnsavedPanels((prev) => {
        const updated = [...prev];
        for (const item of items) {
          const node = item.gridstackNode;
          if (!node) continue;
          const panelId = item.getAttribute("gs-id");
          const idx = updated.findIndex((p) => p.id === panelId);
          if (idx >= 0) {
            updated[idx] = {
              ...updated[idx],
              position: {
                x: node.x ?? updated[idx].position.x,
                y: node.y ?? updated[idx].position.y,
                w: node.w ?? updated[idx].position.w,
                h: node.h ?? updated[idx].position.h,
              },
            };
          }
        }
        return updated;
      });
    });

    return () => {
      gs.destroy(false);
      gsRef.current = null;
      registeredPanels.current = new Set();
    };
  }, [editMode]);

  // Register new panels with GridStack after Preact renders them
  useEffect(() => {
    const gs = gsRef.current;
    const el = gridRef.current;
    if (!gs || !el) return;

    // Register any new items that Preact rendered but GridStack doesn't know about
    const items = el.querySelectorAll<HTMLElement>(".grid-stack-item");
    for (const item of items) {
      const id = item.getAttribute("gs-id");
      if (id && !registeredPanels.current.has(id)) {
        gs.makeWidget(item);
        registeredPanels.current.add(id);
      }
    }

    // Clean up registered panels that were removed from state
    const currentIds = new Set(unsavedPanels.map((p) => p.id));
    for (const id of registeredPanels.current) {
      if (!currentIds.has(id)) {
        registeredPanels.current.delete(id);
      }
    }
  }, [unsavedPanels]);

  // Auto-refresh
  useEffect(() => {
    if (refreshInterval <= 0) return;
    const id = setInterval(() => {
      setRefreshTick((t) => t + 1);
    }, refreshInterval);
    return () => clearInterval(id);
  }, [refreshInterval]);

  // Re-trigger all panels on time change
  const handleTimeChange = useCallback((newFrom: string) => {
    setFrom(newFrom);
    localStorage.setItem("lynxdb-dash-timerange", newFrom);
    setRefreshTick((t) => t + 1);
  }, []);

  // Handle variable value change from VariableBar
  function handleVariableChange(name: string, value: string) {
    setVariableValues((prev) => ({ ...prev, [name]: value }));
    setRefreshTick((t) => t + 1);
  }

  // Build filtered variables for query execution (exclude "*" or empty values)
  const activeVariables: Record<string, string> = {};
  for (const [k, v] of Object.entries(variableValues)) {
    if (v && v !== "*") {
      activeVariables[k] = v;
    }
  }

  // The variables to show in the bar (from unsaved in edit mode, or from dashboard)
  const displayVariables = editMode ? unsavedVariables : (dashboard?.variables ?? unsavedVariables);

  // Save handler
  async function handleSave() {
    if (!dashName.trim() || unsavedPanels.length === 0) return;
    try {
      const input = {
        name: dashName.trim(),
        panels: unsavedPanels,
        variables: unsavedVariables.length > 0 ? unsavedVariables : undefined,
      };
      if (dashboardId && dashboard) {
        await updateDashboard(dashboardId, input);
        setDashboard({ ...dashboard, ...input });
        setEditMode(false);
        setShowVariableEditor(false);
      } else {
        const created = await createDashboard(input);
        route(`/dashboards/${created.id}`);
      }
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to save");
    }
  }

  function handleDiscard() {
    if (dashboard) {
      setUnsavedPanels(dashboard.panels);
      setDashName(dashboard.name);
      setUnsavedVariables(dashboard.variables ?? []);
    }
    setEditMode(false);
    setShowVariableEditor(false);
  }

  function handleAddPanel() {
    setEditingPanel(null);
    setShowPanelForm(true);
  }

  function handleEditPanel(panel: DashboardPanel) {
    setEditingPanel(panel);
    setShowPanelForm(true);
  }

  function handlePanelFormSave(panel: DashboardPanel) {
    if (editingPanel) {
      setUnsavedPanels((prev) =>
        prev.map((p) => (p.id === panel.id ? panel : p)),
      );
    } else {
      // Add new panel -- only update Preact state; makeWidget runs via useEffect
      setUnsavedPanels((prev) => [...prev, panel]);
    }
    setShowPanelForm(false);
    setEditingPanel(null);
  }

  async function handleDeleteDashboard() {
    if (!dashboardId) return;
    if (!window.confirm("Delete this dashboard? This action cannot be undone.")) return;
    try {
      await deleteDashboard(dashboardId);
      route("/dashboards");
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to delete dashboard");
    }
  }

  function handleDeletePanel(panelId: string) {
    setUnsavedPanels((prev) => prev.filter((p) => p.id !== panelId));
    const gs = gsRef.current;
    if (gs) {
      const item = gs
        .getGridItems()
        .find((el) => el.getAttribute("gs-id") === panelId);
      if (item) gs.removeWidget(item);
    }
    registeredPanels.current.delete(panelId);
  }

  const saveDisabled = !dashName.trim() || unsavedPanels.length === 0;

  if (loading) {
    return (
      <div class={styles.detail}>
        <div class={styles.loading}>Loading dashboard...</div>
      </div>
    );
  }

  if (error && !dashboard && dashboardId) {
    return (
      <div class={styles.detail}>
        <div class={styles.error}>
          <div>{error}</div>
          <button
            type="button"
            class={styles.retryBtn}
            onClick={() => {
              setError(null);
              setLoading(true);
              fetchDashboard(dashboardId!)
                .then((d) => {
                  setDashboard(d);
                  setDashName(d.name);
                  setUnsavedPanels(d.panels);
                })
                .catch((e) =>
                  setError(
                    e instanceof Error ? e.message : "Failed to load",
                  ),
                )
                .finally(() => setLoading(false));
            }}
          >
            Retry
          </button>
        </div>
      </div>
    );
  }

  return (
    <div class={styles.detail}>
      <DashboardHeader
        name={dashName}
        editable={true}
        onNameChange={setDashName}
        from={from}
        onTimeChange={handleTimeChange}
        refreshInterval={refreshInterval}
        onRefreshIntervalChange={setRefreshInterval}
        editMode={editMode}
        onToggleEdit={() => setEditMode(true)}
        onSave={handleSave}
        onDiscard={handleDiscard}
        saveDisabled={saveDisabled}
        onDelete={dashboardId ? handleDeleteDashboard : undefined}
        onToggleVariables={() => setShowVariableEditor((v) => !v)}
        showVariableEditor={showVariableEditor}
      />

      {editMode && showVariableEditor && (
        <VariableEditor
          variables={unsavedVariables}
          onChange={setUnsavedVariables}
        />
      )}

      {displayVariables.length > 0 && (
        <VariableBar
          variables={displayVariables}
          values={variableValues}
          onChange={handleVariableChange}
        />
      )}

      <div class={styles.gridArea}>
        {unsavedPanels.length === 0 && editMode && (
          <div class={styles.emptyGrid}>
            <div>No panels yet</div>
            <div>Add your first panel to get started</div>
          </div>
        )}

        <div ref={gridRef} class="grid-stack">
          {unsavedPanels.map((panel) => (
            <div
              key={panel.id}
              class="grid-stack-item"
              gs-id={panel.id}
              gs-x={panel.position.x}
              gs-y={panel.position.y}
              gs-w={panel.position.w}
              gs-h={panel.position.h}
            >
              <div class="grid-stack-item-content">
                <PanelRenderer
                  panel={panel}
                  from={from}
                  variables={activeVariables}
                  refreshTick={refreshTick}
                  editMode={editMode}
                  onEdit={() => handleEditPanel(panel)}
                  onDelete={() => handleDeletePanel(panel.id)}
                />
              </div>
            </div>
          ))}
        </div>

        {editMode && (
          <button
            type="button"
            class={styles.addPanelBtn}
            onClick={handleAddPanel}
          >
            + Add Panel
          </button>
        )}
      </div>

      {showPanelForm && (
        <PanelEditForm
          panel={editingPanel ?? undefined}
          onSave={handlePanelFormSave}
          onCancel={() => {
            setShowPanelForm(false);
            setEditingPanel(null);
          }}
        />
      )}
    </div>
  );
}
