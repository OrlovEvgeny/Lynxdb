import { DashboardList } from "../components/dashboards/DashboardList";
import { DashboardDetail } from "../components/dashboards/DashboardDetail";
import { SystemOverview } from "../components/dashboards/SystemOverview";

interface Props {
  path?: string;
  rest?: string;
}

export default function DashboardsView({ rest }: Props) {
  if (!rest || rest === "") return <DashboardList />;
  if (rest === "system") return <SystemOverview />;
  if (rest === "new")
    return <DashboardDetail dashboardId={null} editMode={true} />;
  const parts = rest.split("/");
  return <DashboardDetail dashboardId={parts[0]} />;
}
