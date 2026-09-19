"use client";

import { useState } from "react";

export function JsonInspection({
  title = "Raw API JSON",
  value,
}: {
  title?: string;
  value: unknown;
}) {
  const [copied, setCopied] = useState(false);
  const json = JSON.stringify(value, null, 2);

  const copy = async () => {
    await navigator.clipboard.writeText(json);
    setCopied(true);
    window.setTimeout(() => setCopied(false), 1500);
  };

  return (
    <details className="rounded-lg border border-border bg-card">
      <summary className="cursor-pointer select-none px-4 py-3 text-sm font-medium text-foreground">
        {title}
      </summary>
      <div className="border-t border-border p-4">
        <div className="mb-3 flex items-center justify-between gap-3">
          <p className="text-xs text-muted-foreground">Exact JSON returned by the Trellis API.</p>
          <button
            type="button"
            onClick={copy}
            className="rounded-md border border-border bg-background px-2.5 py-1.5 text-xs font-medium text-foreground hover:bg-accent"
          >
            {copied ? "Copied" : "Copy JSON"}
          </button>
        </div>
        <pre className="max-h-[32rem] overflow-auto whitespace-pre-wrap break-words rounded-md bg-zinc-950 p-4 font-mono text-xs leading-5 text-zinc-100">
          {json}
        </pre>
      </div>
    </details>
  );
}
