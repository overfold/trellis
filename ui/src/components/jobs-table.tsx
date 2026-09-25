"use client";

import Link from "next/link";
import { useJobs } from "@/hooks/use-api";
import { EmptyState } from "./empty-state";
import { Skeleton } from "./skeleton";
import { jobState, jobStateLabel, type JobState } from "@/lib/operations";
import { useConfig } from "./config-provider";

export function JobsTable() {
  const { data: jobs, isLoading, error } = useJobs();
  const { namespace, setNamespace } = useConfig();

  if (isLoading) return <TableSkeleton />;
  if (error) {
    return (
      <EmptyState
        title="Unable to load jobs"
        description="Could not connect to the cluster. Ensure the console connection is configured and reachable."
      />
    );
  }
  if (!jobs || jobs.length === 0) {
    return (
      <EmptyState
        title="No jobs"
        description="No job manifests have been applied in this namespace yet."
      />
    );
  }

  return (
    <div className="overflow-x-auto rounded-lg border border-border">
      <table className="w-full text-sm">
        <thead>
          <tr className="border-b border-border bg-muted/50">
            <th className="px-4 py-3 text-left font-medium text-muted-foreground">
              Name
            </th>
            {!namespace && <th className="px-4 py-3 text-left font-medium text-muted-foreground">Namespace</th>}
            <th className="px-4 py-3 text-right font-medium text-muted-foreground">
              Desired
            </th>
            <th className="px-4 py-3 text-right font-medium text-muted-foreground">
              Running
            </th>
            <th className="px-4 py-3 text-right font-medium text-muted-foreground">
              Healthy
            </th>
            <th className="px-4 py-3 text-right font-medium text-muted-foreground">
              Revision
            </th>
            <th className="px-4 py-3 text-left font-medium text-muted-foreground">
              Summary
            </th>
          </tr>
        </thead>
        <tbody className="divide-y divide-border">
          {jobs.map((job) => {
            const state = jobState(job);

            return (
              <tr
                key={`${job.namespace}/${job.name}`}
                className="transition-colors hover:bg-muted/30"
              >
                <td className="px-4 py-3">
                  <Link
                    href={`/jobs/${encodeURIComponent(job.name)}`}
                    onClick={() => {
                      if (job.namespace) setNamespace(job.namespace);
                    }}
                    className="font-medium text-card-foreground hover:underline"
                  >
                    {job.name}
                  </Link>
                </td>
                {!namespace && <td className="px-4 py-3 font-mono text-xs text-muted-foreground">{job.namespace}</td>}
                <td className="px-4 py-3 text-right tabular-nums text-card-foreground">
                  {job.desired}
                </td>
                <td className="px-4 py-3 text-right tabular-nums text-card-foreground">
                  {job.running}
                </td>
                <td className="px-4 py-3 text-right tabular-nums text-card-foreground">
                  {job.healthy}
                </td>
                <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">
                  {job.revision}
                </td>
                <td className="px-4 py-3">
                  <HealthIndicator
                    state={state}
                  />
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function HealthIndicator({
  state,
}: {
  state: JobState;
}) {
  const style = {
    ready: "text-emerald-600 dark:text-emerald-400",
    converging: "text-amber-600 dark:text-amber-400",
    degraded: "text-red-600 dark:text-red-400",
  }[state];
  const dot = {
    ready: "bg-emerald-500",
    converging: "bg-amber-500",
    degraded: "bg-red-500",
  }[state];
  return (
    <span className={`inline-flex items-center gap-1.5 text-xs ${style}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${dot}`} />
      {jobStateLabel(state)}
    </span>
  );
}

function TableSkeleton() {
  return (
    <div className="overflow-hidden rounded-lg border border-border">
      <div className="border-b border-border bg-muted/50 px-4 py-3">
        <Skeleton className="h-4 w-64" />
      </div>
      {Array.from({ length: 3 }).map((_, i) => (
        <div key={i} className="flex gap-8 border-b border-border px-4 py-3 last:border-0">
          <Skeleton className="h-4 w-28" />
          <Skeleton className="h-4 w-12" />
          <Skeleton className="h-4 w-12" />
          <Skeleton className="h-4 w-12" />
          <Skeleton className="h-4 w-16" />
        </div>
      ))}
    </div>
  );
}
