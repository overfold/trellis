const rawTrellisURL =
  process.env.TRELLIS_API_URL ||
  (process.env.TRELLIS_ADDR && `https://${process.env.TRELLIS_ADDR}`) ||
  "http://localhost:8128";

export const TRELLIS_URL = /^https?:\/\//.test(rawTrellisURL)
  ? rawTrellisURL
  : `https://${rawTrellisURL}`;

export type DashboardAPIAccess = "namespace" | "cluster";
export type DashboardAccessLevel = "read" | "write";
export type DashboardCredentialKind = "bootstrap" | "operator" | "workload";

export interface DashboardCredentialInfo {
  kind: DashboardCredentialKind;
  scope: DashboardAPIAccess;
  access: DashboardAccessLevel;
  namespace?: string;
}

const namespacePattern = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$/;

export function getClusterName(): string {
  return process.env.TRELLIS_CLUSTER_NAME?.trim() || "Trellis cluster";
}

export function hasConfiguredNamespaceAllowlist(): boolean {
  return (process.env.TRELLIS_NAMESPACES || "")
    .split(",")
    .some((namespace) => namespace.trim() !== "");
}

export function getConfiguredNamespaces(): string[] {
  const configured = (process.env.TRELLIS_NAMESPACES || "")
    .split(",")
    .map((namespace) => namespace.trim())
    .filter(Boolean);
  const defaultNamespace = process.env.TRELLIS_NAMESPACE?.trim() || "";
  if (defaultNamespace && !configured.includes(defaultNamespace)) {
    configured.unshift(defaultNamespace);
  }
  return configured.length > 0 ? configured : [defaultNamespace];
}

export function getDefaultNamespace(): string {
  const namespaces = getConfiguredNamespaces();
  const configuredDefault = process.env.TRELLIS_NAMESPACE?.trim() || "";
  return namespaces.includes(configuredDefault) ? configuredDefault : namespaces[0];
}

export async function getDashboardCredentialInfo(): Promise<DashboardCredentialInfo> {
  const res = await fetch(`${TRELLIS_URL}/v1/auth/whoami`, {
    headers: orchestratorHeaders(null),
    cache: "no-store",
  });
  if (!res.ok) {
    throw new Error(`credential introspection failed: ${res.status}`);
  }
  const data = (await res.json()) as Partial<DashboardCredentialInfo>;
  if (
    (data.kind !== "bootstrap" && data.kind !== "operator" && data.kind !== "workload") ||
    (data.scope !== "cluster" && data.scope !== "namespace") ||
    (data.access !== "read" && data.access !== "write")
  ) {
    throw new Error("credential introspection returned an invalid principal");
  }
  return data as DashboardCredentialInfo;
}

export function resolveDashboardNamespace(request: Request):
  | { namespace: string; error?: never }
  | { namespace?: never; error: string } {
  const requested = request.headers.get("X-Trellis-Namespace")?.trim();
  const namespace = requested ?? getDefaultNamespace();
  if (namespace && !namespacePattern.test(namespace)) {
    return { error: `Namespace ${JSON.stringify(namespace)} is invalid` };
  }
  if (
    hasConfiguredNamespaceAllowlist() &&
    !getConfiguredNamespaces().includes(namespace)
  ) {
    return {
      error: `Namespace ${JSON.stringify(namespace)} is not configured for this Console`,
    };
  }
  return { namespace };
}

export function orchestratorHeaders(
  namespace?: string | null,
): HeadersInit {
  const token =
    process.env.TRELLIS_API_TOKEN || process.env.TRELLIS_TOKEN || "";
  const headers: HeadersInit = {
    Authorization: `Bearer ${token}`,
  };
  const selectedNamespace = namespace === undefined ? getDefaultNamespace() : namespace;
  if (selectedNamespace) {
    headers["X-Trellis-Namespace"] = selectedNamespace;
  }
  return headers;
}

export function getAllowWrites(): boolean {
  return process.env.TRELLIS_ALLOW_WRITES === "true";
}
