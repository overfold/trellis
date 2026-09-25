"use client";

import { useState } from "react";
import { useSecrets } from "@/hooks/use-api";
import { useConfig } from "@/components/config-provider";
import { ConfirmDialog } from "@/components/confirm-dialog";
import { EmptyState } from "@/components/empty-state";
import { Skeleton } from "@/components/skeleton";
import { JsonInspection } from "@/components/json-inspection";
import { deleteSecret, setSecret } from "@/lib/api";
import type { SecretMetadata } from "@/lib/types";
import { formatBytes, timeAgo } from "@/lib/utils";

export default function SecretsPage() {
  const { data, error, isLoading, mutate } = useSecrets();
  const { allowWrites, apiAccess, namespace } = useConfig();
  const [editor, setEditor] = useState<SecretMetadata | "new" | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<SecretMetadata | null>(null);
  const [deleting, setDeleting] = useState(false);
  const [actionError, setActionError] = useState<string | null>(null);

  if (apiAccess !== "cluster") {
    return (
      <div>
        <div className="mb-6">
          <h1 className="text-xl font-semibold text-foreground">Secrets</h1>
        </div>
        <EmptyState
          title="Cluster access required"
          description="Secret management is available only to consoles using cluster API access."
        />
      </div>
    );
  }

  const confirmDelete = async () => {
    if (!deleteTarget) return;
    setDeleting(true);
    setActionError(null);
    try {
      await deleteSecret(deleteTarget.name, namespace);
      setDeleteTarget(null);
      await mutate();
    } catch (err) {
      setActionError(err instanceof Error ? err.message : "Failed to delete secret");
    } finally {
      setDeleting(false);
    }
  };

  return (
    <div>
      <div className="mb-6 flex items-start justify-between gap-4">
        <div>
          <h1 className="text-xl font-semibold text-foreground">Secrets</h1>
          <p className="mt-1 text-sm text-muted-foreground">
            Write-only secret metadata for namespace <span className="font-mono">{namespace || "unscoped"}</span>
          </p>
        </div>
        {allowWrites && namespace && (
          <button
            type="button"
            onClick={() => {
              setActionError(null);
              setEditor("new");
            }}
            className="rounded-md bg-emerald-600 px-4 py-2 text-sm font-medium text-white hover:bg-emerald-700"
          >
            Set Secret
          </button>
        )}
      </div>

      <div className="mb-5 rounded-lg border border-border bg-card p-4 text-sm text-muted-foreground">
        Trellis never returns secret values. The console can list metadata and, in read-write mode, create, rotate, or delete values. Running allocations retain values already delivered until they are replaced.
      </div>

      {actionError && (
        <p className="mb-4 rounded-md border border-red-500/20 bg-red-500/5 px-3 py-2 text-sm text-red-600 dark:text-red-400">
          {actionError}
        </p>
      )}

      {!namespace ? (
        <EmptyState
          title="Namespace required"
          description="Select a non-empty configured namespace to manage secrets from the console."
        />
      ) : isLoading ? (
        <SecretsSkeleton />
      ) : error ? (
        <EmptyState
          title="Unable to load secrets"
          description={error.message || "Secret metadata requires cluster authorization for the configured namespace."}
        />
      ) : !data || data.length === 0 ? (
        <EmptyState
          title="No secrets"
          description="No secrets have been stored in this namespace yet."
        />
      ) : (
        <div className="overflow-x-auto rounded-lg border border-border">
          <table className="w-full text-sm">
            <thead>
              <tr className="border-b border-border bg-muted/50">
                <th className="px-4 py-3 text-left font-medium text-muted-foreground">Name</th>
                <th className="px-4 py-3 text-right font-medium text-muted-foreground">Version</th>
                <th className="px-4 py-3 text-left font-medium text-muted-foreground">Updated</th>
                <th className="px-4 py-3 text-right font-medium text-muted-foreground">Encrypted size</th>
                <th className="px-4 py-3 text-left font-medium text-muted-foreground">Key</th>
                {allowWrites && (
                  <th className="px-4 py-3 text-right font-medium text-muted-foreground">Actions</th>
                )}
              </tr>
            </thead>
            <tbody className="divide-y divide-border">
              {data.map((secret) => (
                <tr key={secret.name} className="hover:bg-muted/30">
                  <td className="px-4 py-3 font-mono text-sm font-medium text-foreground">{secret.name}</td>
                  <td className="px-4 py-3 text-right tabular-nums text-foreground">{secret.version}</td>
                  <td className="px-4 py-3 text-muted-foreground" title={new Date(secret.updated_at).toLocaleString()}>
                    {timeAgo(secret.updated_at)}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-muted-foreground">{formatBytes(secret.ciphertext_size)}</td>
                  <td className="px-4 py-3 font-mono text-xs text-muted-foreground">{secret.key_id}</td>
                  {allowWrites && (
                    <td className="px-4 py-3 text-right">
                      <div className="flex justify-end gap-2">
                        <button
                          type="button"
                          onClick={() => setEditor(secret)}
                          className="rounded-md border border-border bg-background px-2.5 py-1.5 text-xs font-medium text-foreground hover:bg-accent"
                        >
                          Rotate
                        </button>
                        <button
                          type="button"
                          onClick={() => setDeleteTarget(secret)}
                          className="rounded-md border border-red-500/20 bg-red-500/5 px-2.5 py-1.5 text-xs font-medium text-red-600 hover:bg-red-500/10 dark:text-red-400"
                        >
                          Delete
                        </button>
                      </div>
                    </td>
                  )}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}

      {data && data.length > 0 && (
        <section className="mt-5 space-y-3">
          <div>
            <h2 className="text-sm font-medium text-foreground">Secret metadata inspection</h2>
            <p className="mt-1 text-xs text-muted-foreground">API metadata only; secret values are never returned.</p>
          </div>
          {data.map((secret) => <JsonInspection key={secret.name} title={`Raw metadata JSON: ${secret.name}`} value={secret} />)}
        </section>
      )}

      {editor && (
        <SecretEditor
          namespace={namespace}
          existing={editor === "new" ? undefined : editor}
          onClose={() => setEditor(null)}
          onSaved={async () => {
            setEditor(null);
            await mutate();
          }}
        />
      )}
      {deleteTarget && (
        <ConfirmDialog
          open
          title={`Delete "${deleteTarget.name}"?`}
          description="New allocations will no longer be able to resolve this secret. Running allocations retain values already delivered."
          confirmLabel={deleting ? "Deleting…" : "Delete Secret"}
          onConfirm={confirmDelete}
          onCancel={() => {
            if (!deleting) setDeleteTarget(null);
          }}
          danger
        />
      )}
    </div>
  );
}

function SecretEditor({
  namespace,
  existing,
  onClose,
  onSaved,
}: {
  namespace: string;
  existing?: SecretMetadata;
  onClose: () => void;
  onSaved: () => Promise<void>;
}) {
  const [name, setName] = useState(existing?.name ?? "");
  const [value, setValue] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const save = async (event: React.FormEvent) => {
    event.preventDefault();
    if (!name.trim()) {
      setError("Secret name is required.");
      return;
    }
    setSaving(true);
    setError(null);
    try {
      await setSecret(name.trim(), value, namespace, existing?.version);
      await onSaved();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Failed to store secret");
    } finally {
      setSaving(false);
    }
  };

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center p-4" role="dialog" aria-modal="true">
      <button type="button" aria-label="Close" className="absolute inset-0 bg-black/50" onClick={onClose} />
      <form onSubmit={save} className="relative w-full max-w-lg rounded-xl border border-border bg-card shadow-2xl">
        <div className="border-b border-border px-5 py-4">
          <h2 className="font-semibold text-foreground">{existing ? `Rotate ${existing.name}` : "Set Secret"}</h2>
          <p className="mt-1 text-xs text-muted-foreground">The value is sent to Trellis and is never readable back from the API.</p>
        </div>
        <div className="space-y-4 px-5 py-4">
          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">Name</label>
            <input
              value={name}
              disabled={!!existing}
              onChange={(event) => setName(event.target.value)}
              className="w-full rounded-md border border-border bg-background px-3 py-2 font-mono text-sm text-foreground disabled:opacity-60"
              placeholder="database-password"
            />
          </div>
          <div>
            <label className="mb-1 block text-xs font-medium text-muted-foreground">Value</label>
            <textarea
              value={value}
              onChange={(event) => setValue(event.target.value)}
              rows={6}
              autoFocus={!!existing}
              className="w-full resize-y rounded-md border border-border bg-background px-3 py-2 font-mono text-sm text-foreground"
              placeholder="Secret value"
            />
          </div>
          {error && <p className="text-sm text-red-600 dark:text-red-400">{error}</p>}
        </div>
        <div className="flex justify-end gap-2 border-t border-border px-5 py-4">
          <button type="button" onClick={onClose} disabled={saving} className="rounded-md border border-border px-3 py-2 text-sm font-medium text-foreground hover:bg-accent disabled:opacity-50">
            Cancel
          </button>
          <button type="submit" disabled={saving} className="rounded-md bg-emerald-600 px-3 py-2 text-sm font-medium text-white hover:bg-emerald-700 disabled:opacity-50">
            {saving ? "Saving…" : existing ? "Rotate Secret" : "Set Secret"}
          </button>
        </div>
      </form>
    </div>
  );
}

function SecretsSkeleton() {
  return (
    <div className="overflow-hidden rounded-lg border border-border">
      {Array.from({ length: 4 }).map((_, index) => (
        <div key={index} className="flex gap-8 border-b border-border px-4 py-3 last:border-0">
          <Skeleton className="h-4 w-36" />
          <Skeleton className="h-4 w-12" />
          <Skeleton className="h-4 w-20" />
          <Skeleton className="h-4 w-20" />
        </div>
      ))}
    </div>
  );
}
