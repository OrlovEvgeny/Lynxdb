import { DashboardList } from "../components/dashboards/DashboardList";

interface Props {
  path?: string;
  rest?: string;
}

/** Placeholder for DashboardDetail — will be replaced in Plan 02. */
function DashboardDetail({
  dashboardId,
  editMode,
}: {
  dashboardId: string | null;
  editMode?: boolean;
}) {
  return (
    <div
      style={{
        padding: "var(--space-6)",
        color: "var(--text-secondary)",
      }}
    >
      {editMode
        ? "Creating new dashboard..."
        : `Loading dashboard ${dashboardId ?? ""}...`}
    </div>
  );
}

export default function DashboardsView({ rest }: Props) {
  if (!rest || rest === "") return <DashboardList />;
  if (rest === "new")
    return <DashboardDetail dashboardId={null} editMode={true} />;
  const parts = rest.split("/");
  return <DashboardDetail dashboardId={parts[0]} />;
}
