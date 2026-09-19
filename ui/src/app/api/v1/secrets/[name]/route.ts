import { NextRequest, NextResponse } from "next/server";
import {
  orchestratorHeaders,
  TRELLIS_URL,
  getAllowWrites,
  resolveDashboardNamespace,
} from "@/lib/orchestrator";

function namespacePath(namespace: string, name: string) {
  return `${TRELLIS_URL}/v1/namespaces/${encodeURIComponent(namespace)}/secrets/${encodeURIComponent(name)}`;
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ name: string }> },
) {
  if (!getAllowWrites()) {
    return NextResponse.json(
      { error: "Console is read-only" },
      { status: 403 },
    );
  }
  const { name } = await params;
  const selected = resolveDashboardNamespace(request);
  if (selected.error) {
    return NextResponse.json({ error: selected.error }, { status: 403 });
  }
  if (!selected.namespace) {
    return NextResponse.json(
      { error: "A non-empty Console namespace is required for secret management" },
      { status: 400 },
    );
  }
  const url = namespacePath(selected.namespace, name);
  try {
    const body = await request.json();
    const res = await fetch(url, {
      method: "PUT",
      headers: {
        ...orchestratorHeaders(selected.namespace),
        "Content-Type": "application/json",
      },
      body: JSON.stringify(body),
    });
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

export async function DELETE(
  request: Request,
  { params }: { params: Promise<{ name: string }> },
) {
  if (!getAllowWrites()) {
    return NextResponse.json(
      { error: "Console is read-only" },
      { status: 403 },
    );
  }
  const { name } = await params;
  const selected = resolveDashboardNamespace(request);
  if (selected.error) {
    return NextResponse.json({ error: selected.error }, { status: 403 });
  }
  if (!selected.namespace) {
    return NextResponse.json(
      { error: "A non-empty Console namespace is required for secret management" },
      { status: 400 },
    );
  }
  const url = namespacePath(selected.namespace, name);
  try {
    const res = await fetch(url, {
      method: "DELETE",
      headers: orchestratorHeaders(selected.namespace),
    });
    if (!res.ok) {
      const text = await res.text();
      return NextResponse.json(
        { error: text || `Upstream error: ${res.status}` },
        { status: res.status },
      );
    }
    return new NextResponse(null, { status: 204 });
  } catch {
    return NextResponse.json(
      { error: "Failed to connect to orchestrator" },
      { status: 502 },
    );
  }
}
