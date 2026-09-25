"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { useJob } from "@/hooks/use-api";
import { useConfig } from "@/components/config-provider";
import { formatJobManifest } from "@/lib/manifest";
import { StatCard } from "./stat-card";
import { AllocationsTable } from "./allocations-table";
import { Skeleton } from "./skeleton";
import { EmptyState } from "./empty-state";
import { JobForm } from "./job-form";
import { ConfirmDialog } from "./confirm-dialog";
import { JsonInspection } from "./json-inspection";
import { deleteJob } from "@/lib/api";
import {
  attentionAllocations,
  humanizeReason,
  jobState,
  jobStateDescription,
  jobStateLabel,
} from "@/lib/operations";

export function JobDetail({
  name,
  initialAllocationId,
}: {
  name: string;
  initialAllocationId?: string;
}) {
  const { data: job, isLoading, error, mutate } = useJob(name);
  const { allowWrites, namespace } = useConfig();
  const router = useRouter();

  const [editOpen, setEditOpen] = useState(false);
  const [deleteOpen, setDeleteOpen] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [deleteError, setDeleteError] = useState<string | null>(null);

  const handleDelete = async () => {
    setDeleting(true);
    setDeleteError(null);
    try {
      await deleteJob(name, namespace);
      setDeleteOpen(false);
      router.push("/jobs");
    } catch (err) {
      setDeleteError(err instanceof Error ? err.message : "Failed to delete job");
      setDeleting(false);
    }
  };

  if (isLoading) return <JobDetailSkeleton />;
  if (error) {
    return (
      <EmptyState
        title="Job not found"
        description={`Could not load job "${name}". It may have been deleted or the cluster API is unreachable.`}
      />
    );
  }
  if (!job) return null;

  const state = jobState(job);
  const problems = attentionAllocations(job);

  return (
    <div className="space-y-6">
      <div className="flex items-center gap-2 text-sm text-muted-foreground">
        <Link href="/jobs" className="transition-colors hover:text-foreground">Jobs</Link>
        <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="1.5">
          <path d="M5 3l4 4-4 4" />
        </svg>
        <span className="font-medium text-foreground">{job.name}</span>
      </div>

      <div className="flex items-center justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold text-foreground">{job.name}</h1>
          <p className="mt-1 text-sm text-muted-foreground">Revision {job.revision}</p>
        </div>
        <div className="flex items-center gap-2">
          <JobStatePill state={state} />
          {allowWrites && job.spec && (
            <button
              type="button"
              onClick={() => setEditOpen(true)}
              className="flex items-center gap-1.5 rounded-md border border-border bg-background px-3 py-1.5 text-sm font-medium text-foreground transition-colors hover:bg-accent"
            >
              <svg width="13" height="13" viewBox="0 0 13 13" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M9 1.5l2.5 2.5-7 7-3 .5.5-3 7-7z" />
              </svg>
              Edit Manifest
            </button>
          )}
          {allowWrites && (
            <button
              type="button"
              onClick={() => setDeleteOpen(true)}
              className="flex items-center gap-1.5 rounded-md border border-red-200 bg-red-50 px-3 py-1.5 text-sm font-medium text-red-700 transition-colors hover:bg-red-100 dark:border-red-500/30 dark:bg-red-500/10 dark:text-red-400 dark:hover:bg-red-500/20"
            >
              <svg width="13" height="13" viewBox="0 0 13 13" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
                <path d="M2 3h9M5 3V2h3v1M4 3v7a1 1 0 001 1h3a1 1 0 001-1V3" />
              </svg>
              Delete
            </button>
          )}
        </div>
      </div>

      <section className={`rounded-lg border p-5 ${
        state === "ready"
          ? "border-emerald-500/30 bg-emerald-500/5"
          : state === "degraded"
            ? "border-red-500/30 bg-red-500/5"
            : "border-amber-500/30 bg-amber-500/5"
      }`}>
        <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-start">
          <div>
            <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Revision {job.revision}</p>
            <h2 className="mt-1 text-base font-semibold text-foreground">
              {state === "ready"
                ? "Revision is healthy"
                : state === "degraded"
                  ? "Revision needs attention"
                  : "Revision is reconciling"}
            </h2>
            <p className="mt-1 text-sm text-muted-foreground">{jobStateDescription(job)}</p>
          </div>
          <p className="shrink-0 text-xs text-muted-foreground">
            {state === "ready"
              ? "No runtime problems detected"
              : state === "degraded"
                ? "Start with the diagnostic below"
                : "This view refreshes every five seconds"}
          </p>
        </div>
      </section>

      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        <StatCard label="Desired" value={job.desired} />
        <StatCard label="Running" value={job.running} />
        <StatCard label="Healthy" value={job.healthy} />
      </div>

      {problems.length > 0 && state !== "ready" && (
        <section>
          <div className="mb-3">
            <h2 className="text-sm font-medium text-foreground">Diagnostics</h2>
            <p className="mt-1 text-xs text-muted-foreground">Allocations that do not match healthy running state for the current rollout.</p>
          </div>
          <div className="divide-y divide-border overflow-hidden rounded-lg border border-border bg-card">
            {problems.map((allocation) => (
              <Link
                key={allocation.id}
                href={`/jobs/${encodeURIComponent(job.name)}?allocation=${encodeURIComponent(allocation.id)}`}
                className="flex flex-col justify-between gap-2 px-4 py-3 transition-colors hover:bg-muted/30 sm:flex-row sm:items-center"
              >
                <div className="min-w-0">
                  <p className="text-sm font-medium text-foreground">{allocation.group}</p>
                  <p className="mt-1 text-xs text-muted-foreground">
                    {allocation.message || (allocation.reason ? humanizeReason(allocation.reason) : `${allocation.phase} · ${allocation.health}`)}
                    {allocation.next_retry_at ? ` · retry ${new Date(allocation.next_retry_at).toLocaleString()}` : ""}
                  </p>
                </div>
                <span className="shrink-0 text-xs font-medium text-emerald-600 dark:text-emerald-400">Events and logs →</span>
              </Link>
            ))}
          </div>
        </section>
      )}

      <div>
        <h2 className="mb-3 text-sm font-medium text-foreground">Allocations</h2>
        <AllocationsTable
          allocations={job.allocations}
          initialAllocationId={initialAllocationId}
          taskGroups={job.spec?.task_groups ?? []}
        />
      </div>

      {job.spec && (
        <details className="rounded-lg border border-border bg-card" open={false}>
          <summary className="cursor-pointer select-none px-5 py-4 text-sm font-medium text-foreground">
            Job manifest
          </summary>
          <div className="border-t border-border p-4">
            <pre className="max-h-[32rem] overflow-auto whitespace-pre-wrap break-words rounded-md bg-zinc-950 p-4 font-mono text-xs leading-5 text-zinc-100">
              {formatJobManifest(job.spec)}
            </pre>
          </div>
        </details>
      )}

      <JsonInspection title="Canonical job JSON" value={job} />

      {allowWrites && job.spec && (
        <JobForm
          open={editOpen}
          initialSpec={job.spec}
          onClose={() => setEditOpen(false)}
          onSuccess={() => {
            setEditOpen(false);
            mutate();
          }}
        />
      )}

      {allowWrites && (
        <>
          <ConfirmDialog
            open={deleteOpen}
            title={`Delete "${job.name}"?`}
            description="This removes the job's desired state and stops its allocations. This action cannot be undone."
            confirmLabel={deleting ? "Deleting…" : "Delete Job"}
            onConfirm={handleDelete}
            onCancel={() => {
              if (!deleting) {
                setDeleteOpen(false);
                setDeleteError(null);
              }
            }}
            danger
          />
          {deleteError && (
            <p className="text-sm text-red-600 dark:text-red-400">{deleteError}</p>
          )}
        </>
      )}
    </div>
  );
}

function JobStatePill({ state }: { state: ReturnType<typeof jobState> }) {
  const styles = {
    ready: "bg-emerald-50 text-emerald-700 ring-emerald-600/20 dark:bg-emerald-500/10 dark:text-emerald-400 dark:ring-emerald-500/20",
    converging: "bg-amber-50 text-amber-700 ring-amber-600/20 dark:bg-amber-500/10 dark:text-amber-400 dark:ring-amber-500/20",
    degraded: "bg-red-50 text-red-700 ring-red-600/20 dark:bg-red-500/10 dark:text-red-400 dark:ring-red-500/20",
  };
  const dots = { ready: "bg-emerald-500", converging: "bg-amber-500", degraded: "bg-red-500" };
  return (
    <span className={`inline-flex items-center gap-1.5 rounded-full px-3 py-1 text-xs font-medium ring-1 ring-inset ${styles[state]}`}>
      <span className={`h-1.5 w-1.5 rounded-full ${dots[state]}`} />
      {jobStateLabel(state)}
    </span>
  );
}

function JobDetailSkeleton() {
  return (
    <div className="space-y-6">
      <Skeleton className="h-4 w-32" />
      <div>
        <Skeleton className="h-6 w-48" />
        <Skeleton className="mt-2 h-4 w-24" />
      </div>
      <div className="grid grid-cols-1 gap-4 sm:grid-cols-3">
        {Array.from({ length: 3 }).map((_, i) => (
          <div key={i} className="rounded-lg border border-border bg-card p-5">
            <Skeleton className="h-3 w-16" />
            <Skeleton className="mt-3 h-7 w-12" />
          </div>
        ))}
      </div>
      <div>
        <Skeleton className="h-4 w-24" />
        <Skeleton className="mt-3 h-40 w-full" />
      </div>
    </div>
  );
}
