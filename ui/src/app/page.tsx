import { DashboardContent } from "@/components/dashboard-content";

export default function OverviewPage() {
  return (
    <div>
      <div className="mb-6">
        <h1 className="text-xl font-semibold text-foreground">Operations</h1>
        <p className="mt-1 text-sm text-muted-foreground">
          Reconciliation state, failures, and the next useful action
        </p>
      </div>
      <DashboardContent />
    </div>
  );
}
