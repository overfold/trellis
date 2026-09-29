import type {
  AllocationEvent,
  Job,
  JobRegistration,
  JobSpec,
  Node,
  SecretMetadata,
} from "./types";
import type { ManifestPlan } from "./manifest-plan";

const API_BASE = process.env.NEXT_PUBLIC_TRELLIS_API_URL || "";

function errorMessage(data: unknown, fallback: string): string {
  if (typeof data !== "object" || data === null) return fallback;
  const record = data as Record<string, unknown>;
  if (Array.isArray(record.issues)) {
    const issues = record.issues
      .map((issue) => {
        if (typeof issue !== "object" || issue === null) return null;
        const item = issue as Record<string, unknown>;
        if (typeof item.message !== "string") return null;
        return typeof item.path === "string" && item.path
          ? `${item.path}: ${item.message}`
          : item.message;
      })
      .filter((value): value is string => value !== null);
    if (issues.length > 0) return issues.join("\n");
  }
  return typeof record.error === "string" ? record.error : fallback;
}

// ApiError carries the HTTP status of a failed dashboard API request so
// callers can react to specific outcomes such as a 409 version conflict.
export class ApiError extends Error {
  constructor(
    message: string,
    readonly status: number,
  ) {
    super(message);
    this.name = "ApiError";
  }
}

async function apiFetch<T>(path: string): Promise<T> {
  const res = await fetch(`${API_BASE}${path}`);
  if (!res.ok) {
    const data: unknown = await res.json().catch(() => null);
    throw new Error(errorMessage(data, `API error: ${res.status} ${res.statusText}`));
  }
  return res.json();
}

async function apiMutation(
  path: string,
  init: RequestInit,
  namespace?: string,
): Promise<Response> {
  const headers = new Headers(init.headers);
  if (namespace !== undefined) headers.set("X-Trellis-Namespace", namespace);
  const res = await fetch(`${API_BASE}${path}`, { ...init, headers });
  if (!res.ok) {
    const data: unknown = await res.json().catch(() => null);
    throw new ApiError(
      errorMessage(data, `API error: ${res.status} ${res.statusText}`),
      res.status,
    );
  }
  return res;
}

export async function fetchNodes(): Promise<Node[]> {
  return apiFetch<Node[]>("/api/v1/nodes");
}

export async function fetchJobs(): Promise<Job[]> {
  return apiFetch<Job[]>("/api/v1/jobs");
}

export async function fetchJob(name: string): Promise<Job> {
  return apiFetch<Job>(`/api/v1/jobs/${encodeURIComponent(name)}`);
}

export async function planJob(spec: JobSpec): Promise<ManifestPlan> {
  const res = await apiMutation(
    "/api/v1/jobs/plan",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ spec }),
    },
    spec.namespace,
  );
  return res.json();
}

export async function fetchAllocationEvents(
  id: string,
): Promise<AllocationEvent[]> {
  return apiFetch<AllocationEvent[]>(
    `/api/v1/allocations/${encodeURIComponent(id)}/events`,
  );
}

export async function fetchAllocationLogs(
  id: string,
  tail = 200,
): Promise<string> {
  const res = await fetch(
    `${API_BASE}/api/v1/allocations/${encodeURIComponent(id)}/logs?tail=${tail}`,
    { cache: "no-store" },
  );
  if (!res.ok) {
    const data: unknown = await res.json().catch(() => null);
    throw new Error(errorMessage(data, `API error: ${res.status} ${res.statusText}`));
  }
  return res.text();
}

// submitJob applies spec only if the job is still at expectedVersion (0
// requires that it does not exist). A concurrent change rejects the apply
// with an ApiError whose status is 409.
export async function submitJob(
  spec: JobSpec,
  expectedVersion: number,
): Promise<JobRegistration> {
  const res = await apiMutation(
    "/api/v1/jobs",
    {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ spec, expected_version: expectedVersion }),
    },
    spec.namespace,
  );
  return res.json();
}

export async function deleteJob(name: string, namespace: string): Promise<void> {
  await apiMutation(
    `/api/v1/jobs/${encodeURIComponent(name)}`,
    { method: "DELETE" },
    namespace,
  );
}

export async function resetReplacementBackoff(
  name: string,
  group: string,
  namespace: string,
): Promise<void> {
  await apiMutation(
    `/api/v1/jobs/${encodeURIComponent(name)}/groups/${encodeURIComponent(group)}/replacement-backoff/reset`,
    { method: "POST" },
    namespace,
  );
}

export async function drainNode(id: string): Promise<void> {
  await apiMutation(`/api/v1/nodes/${encodeURIComponent(id)}/drain`, {
    method: "POST",
  });
}

export async function undrainNode(id: string): Promise<void> {
  await apiMutation(`/api/v1/nodes/${encodeURIComponent(id)}/drain`, {
    method: "DELETE",
  });
}

export async function fetchSecrets(): Promise<SecretMetadata[]> {
  return apiFetch<SecretMetadata[]>("/api/v1/secrets");
}

function encodeUTF8Base64(value: string): string {
  const bytes = new TextEncoder().encode(value);
  let binary = "";
  for (const byte of bytes) binary += String.fromCharCode(byte);
  return btoa(binary);
}

export async function setSecret(
  name: string,
  value: string,
  namespace: string,
  expectedVersion?: number,
): Promise<SecretMetadata> {
  if (new TextEncoder().encode(value).byteLength > 64 << 10) {
    throw new Error("Secret exceeds 65536 bytes");
  }
  const body = {
    value_base64: encodeUTF8Base64(value),
    expected_version: expectedVersion ?? 0,
  };
  const res = await apiMutation(
    `/api/v1/secrets/${encodeURIComponent(name)}`,
    {
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    },
    namespace,
  );
  return res.json();
}

export async function deleteSecret(name: string, namespace: string): Promise<void> {
  await apiMutation(
    `/api/v1/secrets/${encodeURIComponent(name)}`,
    { method: "DELETE" },
    namespace,
  );
}
