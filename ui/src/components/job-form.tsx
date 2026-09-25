"use client";

import { useCallback, useEffect, useMemo, useState } from "react";
import type { JobSpec } from "@/lib/types";
import starterManifest from "@/lib/starter-manifest.json";
import { formatJobManifest, parseJobManifest } from "@/lib/manifest";
import { getManifestSchema, normalizeManifestForAPI } from "@/lib/manifest-schema";
import { planJob, submitJob } from "@/lib/api";
import {
  formatPlanValue,
  planTitle,
  type ManifestPlan,
} from "@/lib/manifest-plan";
import { useConfig } from "./config-provider";
import { ManifestEditor } from "./manifest-editor";

interface JobFormProps {
  open: boolean;
  initialSpec?: JobSpec;
  onClose: () => void;
  onSuccess: () => void;
}

function defaultSpec(namespace: string): JobSpec {
  const spec = JSON.parse(JSON.stringify(starterManifest)) as JobSpec;
  spec.namespace = namespace;
  return spec;
}

export function JobForm({
  open,
  initialSpec,
  onClose,
  onSuccess,
}: JobFormProps) {
  if (!open) return null;
  return (
    <JobFormPanel
      initialSpec={initialSpec}
      onClose={onClose}
      onSuccess={onSuccess}
    />
  );
}

function JobFormPanel({
  initialSpec,
  onClose,
  onSuccess,
}: {
  initialSpec?: JobSpec;
  onClose: () => void;
  onSuccess: () => void;
}) {
  const { namespace } = useConfig();
  const initial = useMemo(
    () =>
      initialSpec
        ? (JSON.parse(JSON.stringify(initialSpec)) as JobSpec)
        : defaultSpec(namespace),
    [initialSpec, namespace],
  );
  const initialSource = useMemo(() => formatJobManifest(initial), [initial]);
  const [source, setSource] = useState(() => initialSource);
  const [submitting, setSubmitting] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [plan, setPlan] = useState<ManifestPlan | null>(null);
  const [plannedSpec, setPlannedSpec] = useState<JobSpec | null>(null);
  const isEditing = !!initialSpec;
  const dirty = source !== initialSource;

  const handleClose = useCallback(() => {
    if (submitting) return;
    if (dirty && !window.confirm("Discard unapplied manifest changes?")) return;
    onClose();
  }, [dirty, submitting, onClose]);

  useEffect(() => {
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === "Escape") handleClose();
    };
    document.addEventListener("keydown", handleKey);
    return () => document.removeEventListener("keydown", handleKey);
  }, [handleClose]);

  useEffect(() => {
    if (!dirty) return;
    const handleBeforeUnload = (event: BeforeUnloadEvent) => {
      event.preventDefault();
    };
    window.addEventListener("beforeunload", handleBeforeUnload);
    return () => window.removeEventListener("beforeunload", handleBeforeUnload);
  }, [dirty]);

  const clearPlan = () => {
    setPlan(null);
    setPlannedSpec(null);
  };

  const markSourceDirty = () => {
    clearPlan();
    setError(null);
  };

  const parse = (): JobSpec => {
    let spec: JobSpec;
    try {
      spec = parseJobManifest(source);
    } catch (err) {
      throw new Error(
        err instanceof Error ? `Invalid YAML: ${err.message}` : "Invalid YAML",
      );
    }
    if (!spec.name || typeof spec.name !== "string") {
      throw new Error("Job name is required.");
    }
    if (!spec.namespace || typeof spec.namespace !== "string") {
      throw new Error("Job namespace is required.");
    }
    if (spec.namespace !== namespace) {
      throw new Error(
        `Manifest namespace ${JSON.stringify(spec.namespace)} does not match active namespace ${JSON.stringify(namespace)}. Change the manifest or select the intended namespace.`,
      );
    }
    if (isEditing && spec.name !== initialSpec!.name) {
      throw new Error("Job name cannot be changed after creation.");
    }
    return spec;
  };

  const formatSource = () => {
    try {
      const spec = parse();
      setSource(formatJobManifest(spec));
      clearPlan();
      setError(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Invalid job manifest");
    }
  };

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();
    setSubmitting(true);
    setError(null);
    try {
      if (plan) {
        if (plan.action === "none") {
          onClose();
          return;
        }
        if (!plannedSpec) {
          throw new Error("The reviewed manifest is no longer available; review the plan again.");
        }
        await submitJob(plannedSpec);
        onSuccess();
        onClose();
        return;
      }

      const schema = await getManifestSchema();
      const spec = normalizeManifestForAPI(schema, parse()) as JobSpec;
      setPlan(await planJob(spec));
      setPlannedSpec(spec);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Unexpected error");
    } finally {
      setSubmitting(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex" role="dialog" aria-modal="true">
      <button
        type="button"
        aria-label="Close job editor"
        className="absolute inset-0 bg-black/50"
        onClick={handleClose}
      />
      <div className="relative ml-auto flex h-full w-full max-w-3xl flex-col bg-card shadow-2xl">
        <div className="flex items-start justify-between border-b border-border px-6 py-4">
          <div>
            <h2 className="text-base font-semibold text-foreground">
              {isEditing ? `Edit Manifest — ${initialSpec!.name}` : "Apply Manifest"}
            </h2>
            <p className="mt-1 text-xs text-muted-foreground">
              Edit the same YAML job manifest used by trellisctl, documentation, and examples.
            </p>
          </div>
          <button
            type="button"
            onClick={handleClose}
            disabled={submitting}
            className="rounded-md p-1 text-muted-foreground hover:bg-accent hover:text-foreground disabled:opacity-50"
          >
            <span className="sr-only">Close</span>
            <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="2">
              <path d="M3 3l12 12M15 3L3 15" />
            </svg>
          </button>
        </div>

        <form onSubmit={handleSubmit} className="flex flex-1 flex-col overflow-hidden">
          <div className="flex-1 overflow-y-auto px-6 py-5">
            <div className="mb-4 rounded-lg border border-border bg-background/60 p-4 text-xs text-muted-foreground">
              <p>
                Active namespace: <span className="font-mono text-foreground">{namespace || "(unscoped)"}</span>. The manifest namespace must match; the console never rewrites it.
              </p>
              <p className="mt-2">
                YAML is a human-authored representation. Duration fields accept strings such as <span className="font-mono">10s</span> and <span className="font-mono">1m30s</span>; memory accepts values such as <span className="font-mono">64MiB</span> and <span className="font-mono">1GiB</span>. The generated authoring schema identifies those conversions; the console produces canonical JSON, then Trellis validates and plans the result.
              </p>
            </div>

            <div className="mb-2 flex items-center justify-between">
              <label htmlFor="job-manifest" className="text-sm font-medium text-foreground">
                Job manifest (YAML)
              </label>
              <button
                type="button"
                onClick={formatSource}
                className="rounded-md border border-border bg-background px-2.5 py-1.5 text-xs font-medium text-foreground hover:bg-accent"
              >
                Format YAML
              </button>
            </div>
            <ManifestEditor
              value={source}
              onChange={setSource}
              onDirty={markSourceDirty}
            />

            {plan && (
              <section className="mt-4 rounded-lg border border-border bg-background/60 p-4">
                <div className="flex items-start justify-between gap-4">
                  <div>
                    <p className="text-xs font-medium uppercase tracking-wider text-muted-foreground">Semantic plan</p>
                    <p className="mt-1 text-sm font-medium text-foreground">{planTitle(plan)}</p>
                  </div>
                  <button
                    type="button"
                    onClick={clearPlan}
                    className="shrink-0 text-xs font-medium text-emerald-600 hover:underline dark:text-emerald-400"
                  >
                    Edit again
                  </button>
                </div>
                {plan.changes.length > 0 && (
                  <div className="mt-3 max-h-64 overflow-auto rounded-md bg-zinc-950 p-3 font-mono text-xs leading-5 text-zinc-100">
                    {plan.changes.map((change, index) => (
                      <div key={`${change.path}-${index}`}>
                        {change.operation === "add" ? (
                          <span>+ {change.path}: {formatPlanValue(change.path, change.after)}</span>
                        ) : change.operation === "remove" ? (
                          <span>- {change.path}: {formatPlanValue(change.path, change.before)}</span>
                        ) : (
                          <span>~ {change.path}: {formatPlanValue(change.path, change.before)} → {formatPlanValue(change.path, change.after)}</span>
                        )}
                      </div>
                    ))}
                  </div>
                )}
              </section>
            )}
          </div>

          <div className="border-t border-border px-6 py-4">
            {error && (
              <p className="mb-3 whitespace-pre-line rounded-md border border-red-500/20 bg-red-500/5 px-3 py-2 text-sm text-red-600 dark:text-red-400">
                {error}
              </p>
            )}
            <div className="flex justify-end gap-3">
              <button
                type="button"
                onClick={handleClose}
                disabled={submitting}
                className="rounded-md border border-border bg-background px-4 py-2 text-sm font-medium text-foreground hover:bg-accent disabled:opacity-50"
              >
                Cancel
              </button>
              <button
                type="submit"
                disabled={submitting}
                className="min-w-[120px] rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50"
              >
                {submitting
                  ? plan?.action === "none"
                    ? "Closing…"
                    : plan
                      ? "Applying…"
                      : "Planning…"
                  : plan?.action === "none"
                    ? "Close"
                    : plan
                      ? "Apply Manifest"
                      : "Review Plan"}
              </button>
            </div>
          </div>
        </form>
      </div>
    </div>
  );
}
