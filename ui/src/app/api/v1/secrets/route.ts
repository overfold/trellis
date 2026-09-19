import { NextRequest, NextResponse } from "next/server";
import {
  orchestratorHeaders,
  TRELLIS_URL,
  resolveDashboardNamespace,
} from "@/lib/orchestrator";

export async function GET(request: NextRequest) {
  const selected = resolveDashboardNamespace(request);
  if (selected.error) {
    return NextResponse.json({ error: selected.error }, { status: 403 });
  }
  const namespace = selected.namespace;
  if (!namespace) {
    return NextResponse.json(
      { error: "A non-empty Console namespace is required for secret management" },
      { status: 400 },
    );
  }
  try {
    const res = await fetch(
      `${TRELLIS_URL}/v1/namespaces/${encodeURIComponent(namespace)}/secrets`,
      { headers: orchestratorHeaders(namespace), cache: "no-store" },
    );
    if (!res.ok) {
      const text = await res.text();
      return NextResponse.json(
        { error: text || `Upstream error: ${res.status}` },
        { status: res.status },
      );
    }
    return NextResponse.json(await res.json(), {
      headers: { "Cache-Control": "no-store" },
    });
  } catch {
    return NextResponse.json(
      { error: "Failed to connect to orchestrator" },
      { status: 502 },
    );
  }
}
