"use client";

import { useEffect, useState } from "react";
import Link from "next/link";
import { useConfig } from "@/components/config-provider";
import { resetReplacementBackoff } from "@/lib/api";
import { humanizeReason } from "@/lib/operations";
import type { Job, ReplacementBackoff } from "@/lib/types";
import { ConfirmDialog } from "./confirm-dialog";

function formatRemaining(milliseconds: number): string {
  const seconds = Math.ceil(milliseconds / 1000);
  if (seconds < 60) return `${seconds}s`;
  const minutes = Math.floor(seconds / 60);
  const rest = seconds % 60;
  return rest === 0 ? `${minutes}m` : `${minutes}m ${rest}s`;
}

export function ReplacementBackoffSection({
  job,
  onReset,
}: {
  job: Job;
  onReset: () => void;
}) {
  const { allowWrites, namespace } = useConfig();
  const [now, setNow] = useState(() => Date.now());
  const [target, setTarget] = useState<ReplacementBackoff | null>(null);
  const [resetting, setResetting] = useState(false);
  const [resetError, setResetError] = useState<{ group: string; message: string } | null>(null);
  const backoffs = job.replacement_backoff ?? [];

  useEffect(() => {
    if (backoffs.length === 0) return;
    const timer = setInterval(() => setNow(Date.now()), 1000);
    return () => clearInterval(timer);
  }, [backoffs.length]);

  if (backoffs.length === 0) return null;

  const handleReset = async () => {
    if (!target) return;
    setResetting(true);
    setResetError(null);
    try {
      await resetReplacementBackoff(job.name, target.group, namespace);
      setTarget(null);
      onReset();
    } catch (err) {
      setResetError({
        group: target.group,
        message: err instanceof Error ? err.message : "Failed to reset replacement backoff",
      });
      setTarget(null);
    } finally {
      setResetting(false);
    }
  };

  return (
    <section>
      <div className="mb-3">
        <h2 className="text-sm font-medium text-foreground">Replacement backoff</h2>
        <p className="mt-1 text-xs text-muted-foreground">
          Task groups whose allocations keep failing. Trellis delays replacing their failed allocations; lost allocations and a higher count are placed immediately.
        </p>
      </div>
      <div className="divide-y divide-border overflow-hidden rounded-lg border border-amber-500/30 bg-card">
        {backoffs.map((backoff) => {
          const next = new Date(backoff.next_replacement_at);
          const remaining = next.getTime() - now;
          return (
            <div key={backoff.group} className="flex flex-col justify-between gap-3 px-4 py-3 sm:flex-row sm:items-center">
              <div className="min-w-0 space-y-1">
                <p className="text-sm font-medium text-foreground">
                  {backoff.group}
                  <span className="ml-2 text-xs font-normal text-muted-foreground">
                    {backoff.failures} consecutive {backoff.failures === 1 ? "failure" : "failures"} · revision {backoff.job_revision}
                  </span>
                </p>
                <p className="text-xs text-muted-foreground">
                  {backoff.message || (backoff.reason ? humanizeReason(backoff.reason) : "Allocation failed")}
                  {backoff.last_allocation_id && (
                    <>
                      {" · "}
                      <Link
                        href={`/jobs/${encodeURIComponent(job.name)}?allocation=${encodeURIComponent(backoff.last_allocation_id)}`}
                        className="font-mono text-emerald-600 hover:underline dark:text-emerald-400"
                      >
                        {backoff.last_allocation_id.slice(-8)}
                      </Link>
                    </>
                  )}
                </p>
                <p className="text-xs text-muted-foreground">
                  {remaining > 0
                    ? `Next replacement in ${formatRemaining(remaining)} (${next.toLocaleString()})`
                    : "Backoff elapsed; failed allocations are being replaced"}
                </p>
                {resetError?.group === backoff.group && (
                  <p className="text-xs text-red-600 dark:text-red-400">{resetError.message}</p>
                )}
              </div>
              {allowWrites && (
                <button
                  type="button"
                  onClick={() => {
                    setResetError(null);
                    setTarget(backoff);
                  }}
                  disabled={resetting}
                  className="shrink-0 rounded-md border border-border bg-background px-3 py-1.5 text-sm font-medium text-foreground transition-colors hover:bg-accent disabled:opacity-50"
                >
                  Reset backoff
                </button>
              )}
            </div>
          );
        })}
      </div>
      {allowWrites && (
        <ConfirmDialog
          open={target !== null}
          title={`Reset replacement backoff for "${target?.group ?? ""}"?`}
          description="Trellis replaces the task group's failed allocations immediately and counts failures from zero. Reset after fixing the cause of the failures."
          confirmLabel={resetting ? "Resetting…" : "Reset Backoff"}
          onConfirm={() => {
            if (!resetting) void handleReset();
          }}
          onCancel={() => {
            if (!resetting) setTarget(null);
          }}
        />
      )}
    </section>
  );
}
