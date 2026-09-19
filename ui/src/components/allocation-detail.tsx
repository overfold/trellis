"use client";

import { useState } from "react";
import type { Allocation } from "@/lib/types";
import { useAllocationEvents, useAllocationLogs } from "@/hooks/use-api";
import { StatusBadge } from "./status-badge";
import { Skeleton } from "./skeleton";
import { timeAgo } from "@/lib/utils";
import { JsonInspection } from "./json-inspection";

export function AllocationDetail({
  allocation,
  tasks,
  onClose,
}: {
  allocation: Allocation;
  tasks: string[];
  onClose: () => void;
}) {
  const [selectedTask, setSelectedTask] = useState<string | null>(tasks[0] ?? null);
  const {
    data: events,
    error: eventsError,
    isLoading: eventsLoading,
  } = useAllocationEvents(allocation.id);
  const {
    data: logs,
    error: logsError,
    isLoading: logsLoading,
    mutate: refreshLogs,
  } = useAllocationLogs(allocation.id, selectedTask);

  const labels = Object.entries(allocation.labels ?? {});

  return (
    <div className="fixed inset-0 z-50 flex" role="dialog" aria-modal="true">
      <button
        type="button"
        aria-label="Close allocation details"
        className="absolute inset-0 bg-black/50"
        onClick={onClose}
      />
      <div className="relative ml-auto flex h-full w-full max-w-3xl flex-col bg-card shadow-2xl">
        <div className="flex items-start justify-between border-b border-border px-6 py-4">
          <div>
            <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">
              Allocation
            </p>
            <h2 className="mt-1 break-all font-mono text-sm font-semibold text-foreground">
              {allocation.id}
            </h2>
          </div>
          <button
            type="button"
            onClick={onClose}
            className="rounded-md p-1 text-muted-foreground hover:bg-accent hover:text-foreground"
          >
            <span className="sr-only">Close</span>
            <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M3 3l12 12M15 3L3 15" />
            </svg>
          </button>
        </div>

        <div className="flex-1 space-y-7 overflow-y-auto px-6 py-5">
          <section>
            <h3 className="mb-3 text-sm font-medium text-foreground">Current state</h3>
            <div className="grid grid-cols-2 gap-3 sm:grid-cols-4">
              <Field label="Lifecycle">
                <StatusBadge status={allocation.phase} />
              </Field>
              <Field label="Health">
                <StatusBadge status={allocation.health} />
              </Field>
              <Field label="Generation" value={String(allocation.generation)} />
              <Field label="Attempt" value={String(allocation.attempt ?? 0)} />
            </div>
          </section>

          {(allocation.reason || allocation.message || allocation.next_retry_at) && (
            <section className="rounded-lg border border-amber-500/20 bg-amber-500/5 p-4">
              <h3 className="text-sm font-medium text-foreground">Diagnostic</h3>
              {allocation.reason && (
                <p className="mt-2 font-mono text-xs text-amber-700 dark:text-amber-300">
                  {allocation.reason}
                </p>
              )}
              {allocation.message && (
                <p className="mt-1 text-sm text-muted-foreground">{allocation.message}</p>
              )}
              {allocation.next_retry_at && (
                <p className="mt-2 text-xs text-muted-foreground">
                  Next retry {timeAgo(allocation.next_retry_at)} ({new Date(allocation.next_retry_at).toLocaleString()})
                </p>
              )}
            </section>
          )}

          <section>
            <h3 className="mb-3 text-sm font-medium text-foreground">Placement</h3>
            <div className="grid grid-cols-1 gap-3 sm:grid-cols-2">
              <Field label="Task group" value={allocation.group} />
              <Field label="Node" value={allocation.node_id || "—"} mono />
              <Field label="Address" value={allocation.address || "—"} mono />
              <Field label="Job revision" value={String(allocation.job_revision ?? "—")} />
              <Field label="Draining" value={allocation.draining ? "yes" : "no"} />
              {allocation.created_at && (
                <Field
                  label="Created"
                  value={`${timeAgo(allocation.created_at)} · ${new Date(allocation.created_at).toLocaleString()}`}
                />
              )}
              {allocation.last_transition_at && (
                <Field
                  label="Last transition"
                  value={`${timeAgo(allocation.last_transition_at)} · ${new Date(allocation.last_transition_at).toLocaleString()}`}
                />
              )}
            </div>

            {(allocation.ports?.length ?? 0) > 0 && (
              <div className="mt-4">
                <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Reserved host ports</p>
                <div className="flex flex-wrap gap-2">
                  {allocation.ports!.map((port, index) => (
                    <span
                      key={`${port.host_port}-${port.container_port}-${index}`}
                      className="rounded-md border border-border bg-background px-2 py-1 font-mono text-xs text-foreground"
                    >
                      {port.host_port}
                    </span>
                  ))}
                </div>
              </div>
            )}

            {labels.length > 0 && (
              <div className="mt-4">
                <p className="mb-2 text-xs font-medium uppercase tracking-wider text-muted-foreground">Labels</p>
                <div className="flex flex-wrap gap-2">
                  {labels.map(([key, value]) => (
                    <span
                      key={key}
                      className="rounded-md bg-muted px-2 py-1 font-mono text-xs text-muted-foreground"
                    >
                      {key}={value}
                    </span>
                  ))}
                </div>
              </div>
            )}
          </section>

          <JsonInspection title="Raw allocation JSON" value={allocation} />

          <section>
            <h3 className="mb-3 text-sm font-medium text-foreground">Lifecycle events</h3>
            {eventsLoading ? (
              <div className="space-y-2">
                <Skeleton className="h-12 w-full" />
                <Skeleton className="h-12 w-full" />
              </div>
            ) : eventsError ? (
              <ErrorText message={eventsError.message} />
            ) : !events || events.length === 0 ? (
              <p className="text-sm text-muted-foreground">No lifecycle events recorded.</p>
            ) : (
              <div className="overflow-hidden rounded-lg border border-border">
                {events.map((event, index) => (
                  <div
                    key={`${event.at}-${index}`}
                    className="flex gap-3 border-b border-border px-4 py-3 last:border-0"
                  >
                    <div className="pt-0.5">
                      <StatusBadge status={event.phase} />
                    </div>
                    <div className="min-w-0 flex-1">
                      <div className="flex flex-wrap items-center gap-x-2 gap-y-1">
                        {event.reason && (
                          <span className="font-mono text-xs text-foreground">{event.reason}</span>
                        )}
                        <span className="text-xs text-muted-foreground" title={new Date(event.at).toLocaleString()}>
                          {timeAgo(event.at)}
                        </span>
                      </div>
                      {event.message && (
                        <p className="mt-1 text-sm text-muted-foreground">{event.message}</p>
                      )}
                    </div>
                  </div>
                ))}
              </div>
            )}
          </section>

          <section>
            <div className="mb-3 flex items-end justify-between gap-4">
              <div>
                <h3 className="text-sm font-medium text-foreground">Logs</h3>
                <p className="mt-0.5 text-xs text-muted-foreground">Last 200 lines for one task, refreshed every five seconds.</p>
              </div>
              <div className="flex items-center gap-2">
                {tasks.length > 1 && (
                  <select
                    value={selectedTask ?? ""}
                    onChange={(event) => setSelectedTask(event.target.value)}
                    className="rounded-md border border-border bg-background px-2.5 py-1.5 font-mono text-xs text-foreground"
                    aria-label="Task log stream"
                  >
                    {tasks.map((task) => (
                      <option key={task} value={task}>{task}</option>
                    ))}
                  </select>
                )}
                {tasks.length === 1 && (
                  <span className="font-mono text-xs text-muted-foreground">{tasks[0]}</span>
                )}
                <button
                  type="button"
                  onClick={() => refreshLogs()}
                  className="rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium text-foreground hover:bg-accent"
                >
                  Refresh
                </button>
              </div>
            </div>
            {logsLoading ? (
              <Skeleton className="h-48 w-full" />
            ) : logsError ? (
              <ErrorText message={logsError.message} />
            ) : (
              <pre className="max-h-96 overflow-auto whitespace-pre-wrap break-words rounded-lg border border-border bg-zinc-950 p-4 font-mono text-xs leading-5 text-zinc-100">
                {logs || "No log output."}
              </pre>
            )}
          </section>
        </div>
      </div>
    </div>
  );
}

function Field({
  label,
  value,
  mono,
  children,
}: {
  label: string;
  value?: string;
  mono?: boolean;
  children?: React.ReactNode;
}) {
  return (
    <div className="rounded-lg border border-border bg-background/50 p-3">
      <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">{label}</p>
      <div className={`mt-1.5 break-all text-sm text-foreground ${mono ? "font-mono text-xs" : ""}`}>
        {children ?? value ?? "—"}
      </div>
    </div>
  );
}

function ErrorText({ message }: { message: string }) {
  return (
    <p className="rounded-md border border-red-500/20 bg-red-500/5 px-3 py-2 text-sm text-red-600 dark:text-red-400">
      {message}
    </p>
  );
}
