"use client";

import { useState } from "react";
import { useNodes } from "@/hooks/use-api";
import { useConfig } from "./config-provider";
import { StatusBadge } from "./status-badge";
import { ConfirmDialog } from "./confirm-dialog";
import { formatCPU, formatBytes, timeAgo } from "@/lib/utils";
import { drainNode, undrainNode } from "@/lib/api";
import { EmptyState } from "./empty-state";
import { Skeleton } from "./skeleton";
import type { Node } from "@/lib/types";

export function NodesTable() {
  const { data: nodes, isLoading, error, mutate } = useNodes();
  const { allowWrites } = useConfig();
  const [drainTarget, setDrainTarget] = useState<Node | null>(null);
  const [undrainingID, setUndrainingID] = useState<string | null>(null);
  const [draining, setDraining] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  const confirmDrain = async () => {
    if (!drainTarget) return;
    setDraining(true);
    setActionError(null);
    try {
      await drainNode(drainTarget.id);
      setDrainTarget(null);
      await mutate();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Failed to drain node");
    } finally {
      setDraining(false);
    }
  };

  const handleUndrain = async (node: Node) => {
    setUndrainingID(node.id);
    setActionError(null);
    try {
      await undrainNode(node.id);
      await mutate();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Failed to undrain node");
    } finally {
      setUndrainingID(null);
    }
  };

  if (isLoading) return <TableSkeleton />;
  if (error) {
    return (
      <EmptyState
        title="Unable to load nodes"
        description="Could not connect to the orchestrator. Ensure it is running and the UI is configured."
      />
    );
  }
  if (!nodes || nodes.length === 0) {
    return (
      <EmptyState
        title="No nodes"
        description="No nodes have registered with the orchestrator yet."
      />
    );
  }

  return (
    <>
      {actionError && (
        <p className="mb-3 rounded-md border border-red-500/20 bg-red-500/5 px-3 py-2 text-sm text-red-600 dark:text-red-400">
          {actionError}
        </p>
      )}
      <div className="overflow-x-auto rounded-lg border border-border">
        <table className="w-full text-sm">
          <thead>
            <tr className="border-b border-border bg-muted/50">
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">Host</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">Status</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">Capacity</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">Placement</th>
              <th className="px-4 py-3 text-left font-medium text-muted-foreground">Heartbeat</th>
              {allowWrites && (
                <th className="px-4 py-3 text-right font-medium text-muted-foreground">Actions</th>
              )}
            </tr>
          </thead>
          <tbody className="divide-y divide-border">
            {nodes.map((node) => {
              const labels = Object.entries(node.labels ?? {});
              const volumes = node.volumes ?? [];
              const capabilities = node.capabilities ?? [];
              return (
                <tr key={node.id} className="align-top transition-colors hover:bg-muted/30">
                  <td className="px-4 py-3">
                    <p className="font-medium text-card-foreground">{node.host}:{node.port}</p>
                    <p className="mt-0.5 font-mono text-xs text-muted-foreground" title={node.id}>
                      {node.id.substring(0, 8)}
                    </p>
                  </td>
                  <td className="px-4 py-3">
                    <StatusBadge status={node.status} />
                  </td>
                  <td className="px-4 py-3 text-card-foreground">
                    <p className="tabular-nums">{formatCPU(node.cpu)}</p>
                    <p className="mt-0.5 tabular-nums text-xs text-muted-foreground">{formatBytes(node.memory)}</p>
                  </td>
                  <td className="max-w-sm px-4 py-3">
                    {labels.length === 0 && volumes.length === 0 && capabilities.length === 0 ? (
                      <span className="text-muted-foreground">—</span>
                    ) : (
                      <div className="space-y-2">
                        {labels.length > 0 && (
                          <div className="flex flex-wrap gap-1">
                            {labels.map(([key, value]) => (
                              <span key={key} className="rounded bg-muted px-1.5 py-0.5 font-mono text-[11px] text-muted-foreground">
                                {key}={value}
                              </span>
                            ))}
                          </div>
                        )}
                        {volumes.length > 0 && (
                          <p className="text-xs text-muted-foreground">
                            Volumes: {volumes.join(", ")}
                          </p>
                        )}
                        {capabilities.length > 0 && (
                          <p className="text-xs text-muted-foreground">
                            Capabilities: {capabilities.join(", ")}
                          </p>
                        )}
                      </div>
                    )}
                  </td>
                  <td className="whitespace-nowrap px-4 py-3 text-muted-foreground" title={new Date(node.last_heartbeat).toLocaleString()}>
                    {timeAgo(node.last_heartbeat)}
                  </td>
                  {allowWrites && (
                    <td className="px-4 py-3 text-right">
                      {node.status === "draining" ? (
                        <button
                          type="button"
                          disabled={undrainingID === node.id}
                          onClick={() => handleUndrain(node)}
                          className="rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium text-foreground hover:bg-accent disabled:opacity-50"
                        >
                          {undrainingID === node.id ? "Undraining…" : "Undrain"}
                        </button>
                      ) : (
                        <button
                          type="button"
                          onClick={() => {
                            setActionError(null);
                            setDrainTarget(node);
                          }}
                          className="rounded-md border border-border bg-background px-3 py-1.5 text-xs font-medium text-foreground hover:bg-accent"
                        >
                          Drain
                        </button>
                      )}
                    </td>
                  )}
                </tr>
              );
            })}
          </tbody>
        </table>
      </div>
      {allowWrites && drainTarget && (
        <ConfirmDialog
          open
          title={`Drain ${drainTarget.host}?`}
          description="Trellis will stop scheduling new allocations on this node and migrate its existing allocations where capacity permits."
          confirmLabel={draining ? "Draining…" : "Drain Node"}
          onConfirm={confirmDrain}
          onCancel={() => {
            if (!draining) setDrainTarget(null);
          }}
          danger
        />
      )}
    </>
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
          <Skeleton className="h-8 w-32" />
          <Skeleton className="h-5 w-16" />
          <Skeleton className="h-8 w-24" />
          <Skeleton className="h-8 w-48" />
          <Skeleton className="h-4 w-16" />
        </div>
      ))}
    </div>
  );
}
