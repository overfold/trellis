"use client";

import { useState } from "react";
import Link from "next/link";
import { useJobs, useNodes, useOrchestratorStatus } from "@/hooks/use-api";
import { useConfig } from "./config-provider";
import { JobForm } from "./job-form";
import { ResourceBar } from "./resource-bar";
import { Skeleton } from "./skeleton";
import {
  jobState,
  jobStateDescription,
  jobStateLabel,
  operationalIssues,
  type IssueSeverity,
  type JobState,
  type OperationalIssue,
} from "@/lib/operations";
import type { Job, Node } from "@/lib/types";

export function DashboardContent() {
  const { data: nodes, error: nodesError, isLoading: nodesLoading } = useNodes();
  const {
    data: jobs,
    error: jobsError,
    isLoading: jobsLoading,
    mutate: refreshJobs,
  } = useJobs();
  const cluster = useOrchestratorStatus();
  const { allowWrites, apiAccess, clusterName, namespace, setNamespace } = useConfig();
  const [formOpen, setFormOpen] = useState(false);
  const clusterScope = apiAccess === "cluster";

  if (jobsLoading || (clusterScope && nodesLoading)) return <DashboardSkeleton />;

  const nodeList = clusterScope ? (nodes ?? []) : [];
  const jobList = jobs ?? [];
  const issues = operationalIssues(jobList, nodeList);
  const actionIssues = issues.filter((issue) => issue.severity !== "info");
  const activity = issues.filter((issue) => issue.severity === "info");
  const states = countStates(jobList);
  const healthyNodes = nodeList.filter((node) => node.status === "healthy").length;
  const { totalCPU, totalMemory } = aggregateResources(nodeList);
  const { requestedCPU, requestedMemory } = requestedResources(jobList);
  const disconnected = !!jobsError || cluster.error || (clusterScope && !!nodesError);
  const summary = operationalSummary({
    disconnected,
    actionIssues,
    converging: states.converging,
    healthyNodes,
    totalNodes: nodeList.length,
    clusterScope,
  });

  return (
    <div className="space-y-6">
      <section className={`rounded-xl border p-5 ${summary.style}`}>
        <div className="flex flex-col justify-between gap-4 sm:flex-row sm:items-start">
          <div className="flex gap-3">
            <span className={`mt-1 h-2.5 w-2.5 shrink-0 rounded-full ${summary.dot}`} />
            <div>
              <p className="text-lg font-semibold text-foreground">{summary.title}</p>
              <p className="mt-1 max-w-2xl text-sm text-muted-foreground">{summary.description}</p>
              <p className="mt-2 text-xs text-muted-foreground">
                <span className="font-medium text-foreground">{clusterName}</span>
                <span className="mx-1.5">/</span>
                <span className="font-mono">{namespace || "unscoped"}</span>
              </p>
            </div>
          </div>
          {allowWrites && (
            <button
              type="button"
              onClick={() => setFormOpen(true)}
              className="shrink-0 rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white transition-colors hover:bg-emerald-700"
            >
              Apply Manifest
            </button>
          )}
        </div>
      </section>

      <div className={`grid grid-cols-1 gap-3 sm:grid-cols-3 ${clusterScope ? "lg:grid-cols-4" : ""}`}>
        <StateCard label="Ready" value={states.ready} tone="ready" detail="jobs at desired health" />
        <StateCard label="Reconciling" value={states.converging} tone="converging" detail="jobs advancing toward desired state" />
        <StateCard label="Degraded" value={states.degraded} tone="degraded" detail="jobs with explicit failures" />
        {clusterScope && (
          <StateCard
            label="Nodes"
            value={`${healthyNodes}/${nodeList.length}`}
            tone={healthyNodes === nodeList.length ? "ready" : "degraded"}
            detail="healthy cluster nodes"
          />
        )}
      </div>

      <section className="rounded-lg border border-border bg-card">
        <SectionHeader title="Action required" detail="Failures and blocked progress with the next useful place to look." count={actionIssues.length} />
        {disconnected ? (
          <div className="px-5 py-5">
            <p className="text-sm font-medium text-red-600 dark:text-red-400">Cluster data is unavailable</p>
            <p className="mt-1 text-sm text-muted-foreground">Check the console service connection and the configured cluster API node.</p>
          </div>
        ) : actionIssues.length === 0 ? (
          <div className="flex items-start gap-3 px-5 py-5">
            <span className="mt-0.5 flex h-5 w-5 items-center justify-center rounded-full bg-emerald-500/10 text-emerald-600 dark:text-emerald-400">✓</span>
            <div>
              <p className="text-sm font-medium text-foreground">No explicit failures detected</p>
              <p className="mt-1 text-sm text-muted-foreground">
                {states.converging > 0
                  ? "Some job revisions are still reconciling; their progress is shown below."
                  : clusterScope
                    ? "Desired capacity is healthy and no nodes require attention."
                    : "Desired workload capacity in this namespace is healthy."}
              </p>
            </div>
          </div>
        ) : (
          <div className="divide-y divide-border">
            {actionIssues.map((issue) => <IssueRow key={issue.id} issue={issue} />)}
          </div>
        )}
      </section>

      <section className="rounded-lg border border-border bg-card">
        <div className="flex items-center justify-between border-b border-border px-5 py-4">
          <div>
            <h2 className="text-sm font-semibold text-card-foreground">Jobs</h2>
            <p className="mt-0.5 text-xs text-muted-foreground">Current job revision progress in this namespace.</p>
          </div>
          <Link href="/jobs" className="text-xs font-medium text-emerald-600 hover:underline dark:text-emerald-400">View all jobs</Link>
        </div>
        {jobList.length === 0 ? (
          <div className="px-5 py-8 text-center">
            <p className="text-sm font-medium text-foreground">No jobs applied</p>
            <p className="mt-1 text-sm text-muted-foreground">Apply a YAML job manifest to create desired state.</p>
          </div>
        ) : (
          <div className="divide-y divide-border">
            {[...jobList].sort(compareJobs).map((job) => <JobRow key={`${job.namespace}/${job.name}`} job={job} setNamespace={setNamespace} />)}
          </div>
        )}
      </section>

      {activity.length > 0 && (
        <section className="rounded-lg border border-border bg-card">
          <SectionHeader title="Cluster activity" detail="In-progress maintenance and other non-failing operations." />
          <div className="divide-y divide-border">
            {activity.map((issue) => <IssueRow key={issue.id} issue={issue} />)}
          </div>
        </section>
      )}

      {clusterScope && (
        <details className="rounded-lg border border-border bg-card">
          <summary className="cursor-pointer select-none px-5 py-4 text-sm font-medium text-card-foreground">Capacity and reservation pressure</summary>
          <div className="space-y-4 border-t border-border px-5 py-5">
            <ResourceBar label="CPU requested" used={requestedCPU} total={totalCPU} format="cpu" />
            <ResourceBar label="Memory requested" used={requestedMemory} total={totalMemory} format="memory" />
            <p className="text-xs text-muted-foreground">Desired job reservations versus schedulable cluster capacity, not live container utilization.</p>
          </div>
        </details>
      )}

      {allowWrites && (
        <JobForm open={formOpen} onClose={() => setFormOpen(false)} onSuccess={() => refreshJobs()} />
      )}
    </div>
  );
}

function SectionHeader({ title, detail, count }: { title: string; detail: string; count?: number }) {
  return (
    <div className="flex items-center justify-between border-b border-border px-5 py-4">
      <div>
        <h2 className="text-sm font-semibold text-card-foreground">{title}</h2>
        <p className="mt-0.5 text-xs text-muted-foreground">{detail}</p>
      </div>
      {!!count && (
        <span className="rounded-full bg-red-500/10 px-2.5 py-1 text-xs font-medium text-red-600 dark:text-red-400">{count}</span>
      )}
    </div>
  );
}

function IssueRow({ issue }: { issue: OperationalIssue }) {
  const tones: Record<IssueSeverity, string> = { critical: "bg-red-500", warning: "bg-amber-500", info: "bg-blue-500" };
  return (
    <div className="flex flex-col justify-between gap-3 px-5 py-4 sm:flex-row sm:items-center">
      <div className="flex min-w-0 gap-3">
        <span className={`mt-1.5 h-2 w-2 shrink-0 rounded-full ${tones[issue.severity]}`} />
        <div className="min-w-0">
          <p className="text-sm font-medium text-foreground">{issue.title}</p>
          <p className="mt-1 text-sm text-muted-foreground">{issue.description}</p>
        </div>
      </div>
      <Link href={issue.href} className="ml-5 shrink-0 self-start text-xs font-medium text-emerald-600 hover:underline dark:text-emerald-400 sm:self-auto">
        {issue.action} →
      </Link>
    </div>
  );
}

function JobRow({ job, setNamespace }: { job: Job; setNamespace: (namespace: string) => void }) {
  const state = jobState(job);
  const tone: Record<JobState, string> = { ready: "bg-emerald-500", converging: "bg-amber-500", degraded: "bg-red-500" };
  const pct = job.desired > 0 ? Math.min(100, Math.round((job.healthy / job.desired) * 100)) : 0;
  return (
    <Link
      href={`/jobs/${encodeURIComponent(job.name)}`}
      onClick={() => {
        if (job.namespace) setNamespace(job.namespace);
      }}
      className="block px-5 py-4 transition-colors hover:bg-muted/30"
    >
      <div className="flex flex-col justify-between gap-3 sm:flex-row sm:items-center">
        <div className="min-w-0">
          <div className="flex items-center gap-2">
            <span className={`h-2 w-2 rounded-full ${tone[state]}`} />
            <p className="truncate text-sm font-medium text-foreground">{job.name}</p>
            <span className="text-xs text-muted-foreground">rev {job.revision}</span>
          </div>
          <p className="mt-1 text-xs text-muted-foreground">{jobStateDescription(job)}</p>
        </div>
        <div className="flex shrink-0 items-center gap-3">
          <div className="h-1.5 w-24 overflow-hidden rounded-full bg-muted">
            <div className={`h-full rounded-full ${tone[state]}`} style={{ width: `${pct}%` }} />
          </div>
          <span className="w-20 text-right text-xs font-medium text-foreground">{jobStateLabel(state)}</span>
        </div>
      </div>
    </Link>
  );
}

function StateCard({ label, value, detail, tone }: { label: string; value: string | number; detail: string; tone: JobState }) {
  const border: Record<JobState, string> = { ready: "border-l-emerald-500", converging: "border-l-amber-500", degraded: "border-l-red-500" };
  return (
    <div className={`rounded-lg border border-l-2 border-border bg-card p-4 ${border[tone]}`}>
      <p className="text-xs font-medium text-muted-foreground">{label}</p>
      <p className="mt-1 text-2xl font-semibold tracking-tight text-card-foreground">{value}</p>
      <p className="mt-1 text-xs text-muted-foreground">{detail}</p>
    </div>
  );
}

function operationalSummary({ disconnected, actionIssues, converging, healthyNodes, totalNodes, clusterScope }: {
  disconnected: boolean;
  actionIssues: OperationalIssue[];
  converging: number;
  healthyNodes: number;
  totalNodes: number;
  clusterScope: boolean;
}) {
  if (disconnected) return { title: "Cluster unavailable", description: "The console cannot currently read its authorized Trellis state.", style: "border-red-500/30 bg-red-500/5", dot: "bg-red-500" };
  const critical = actionIssues.filter((issue) => issue.severity === "critical").length;
  if (critical > 0) return { title: "Cluster needs attention", description: `${critical} explicit failure${critical === 1 ? "" : "s"} detected. Start with the diagnostics below.`, style: "border-red-500/30 bg-red-500/5", dot: "bg-red-500" };
  if (actionIssues.length > 0) return { title: "Progress is blocked", description: `${actionIssues.length} job${actionIssues.length === 1 ? "" : "s"} report a placement or retry condition.`, style: "border-amber-500/30 bg-amber-500/5", dot: "bg-amber-500" };
  if (converging > 0) return { title: "Reconciliation in progress", description: `${converging} job${converging === 1 ? " is" : "s are"} advancing toward desired state.`, style: "border-amber-500/30 bg-amber-500/5", dot: "bg-amber-500" };
  if (clusterScope && (totalNodes === 0 || healthyNodes === 0)) return { title: "No healthy nodes", description: "Register a healthy node before applying workloads.", style: "border-amber-500/30 bg-amber-500/5", dot: "bg-amber-500" };
  return { title: "All systems ready", description: clusterScope ? "Desired workload capacity is healthy and no cluster problems are reported." : "Desired workload capacity in this namespace is healthy.", style: "border-emerald-500/30 bg-emerald-500/5", dot: "bg-emerald-500" };
}

function countStates(jobs: Job[]): Record<JobState, number> {
  return jobs.reduce<Record<JobState, number>>((counts, job) => {
    counts[jobState(job)] += 1;
    return counts;
  }, { ready: 0, converging: 0, degraded: 0 });
}

function compareJobs(a: Job, b: Job): number {
  const rank: Record<JobState, number> = { degraded: 0, converging: 1, ready: 2 };
  return rank[jobState(a)] - rank[jobState(b)] || a.name.localeCompare(b.name);
}

function aggregateResources(nodes: Node[]) {
  let totalCPU = 0;
  let totalMemory = 0;
  for (const node of nodes) {
    if (node.status !== "draining") {
      totalCPU += node.cpu;
      totalMemory += node.memory;
    }
  }
  return { totalCPU, totalMemory };
}

function requestedResources(jobs: Job[]) {
  let requestedCPU = 0;
  let requestedMemory = 0;
  for (const job of jobs) {
    for (const group of job.spec?.task_groups ?? []) {
      for (const task of group.tasks) {
        if (!task.resources) continue;
        requestedCPU += task.resources.cpu * group.count;
        requestedMemory += task.resources.memory * group.count;
      }
    }
  }
  return { requestedCPU, requestedMemory };
}

function DashboardSkeleton() {
  return (
    <div className="space-y-6">
      <div className="rounded-xl border border-border bg-card p-5">
        <Skeleton className="h-6 w-48" />
        <Skeleton className="mt-3 h-4 w-96 max-w-full" />
      </div>
      <div className="grid grid-cols-2 gap-3 lg:grid-cols-4">
        {Array.from({ length: 4 }).map((_, index) => (
          <div key={index} className="rounded-lg border border-border bg-card p-4">
            <Skeleton className="h-3 w-20" />
            <Skeleton className="mt-3 h-7 w-12" />
          </div>
        ))}
      </div>
      <Skeleton className="h-48 w-full" />
      <Skeleton className="h-56 w-full" />
    </div>
  );
}
