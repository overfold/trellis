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

const MAX_SECRET_BYTES = 64 << 10;
const MAX_SECRET_BASE64_LENGTH = Math.ceil(MAX_SECRET_BYTES / 3) * 4;
const MAX_SECRET_REQUEST_BYTES = 96 << 10;

async function readSecretRequest(request: NextRequest) {
  const contentLength = Number(request.headers.get("content-length"));
  if (Number.isFinite(contentLength) && contentLength > MAX_SECRET_REQUEST_BYTES) {
    throw new RangeError("Secret request is too large");
  }
  if (!request.body) throw new SyntaxError("Missing request body");

  const reader = request.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    size += value.byteLength;
    if (size > MAX_SECRET_REQUEST_BYTES) {
      await reader.cancel();
      for (const chunk of chunks) chunk.fill(0);
      value.fill(0);
      throw new RangeError("Secret request is too large");
    }
    chunks.push(value);
  }
  const bytes = new Uint8Array(size);
  let offset = 0;
  for (const chunk of chunks) {
    bytes.set(chunk, offset);
    offset += chunk.byteLength;
    chunk.fill(0);
  }
  let body: { value_base64?: unknown; expected_version?: unknown };
  try {
    body = JSON.parse(new TextDecoder().decode(bytes));
  } finally {
    bytes.fill(0);
  }
  if (
    typeof body.value_base64 !== "string" ||
    body.value_base64.length > MAX_SECRET_BASE64_LENGTH ||
    Math.floor(body.value_base64.length / 4) * 3 -
      (body.value_base64.endsWith("==")
        ? 2
        : body.value_base64.endsWith("=")
          ? 1
          : 0) >
      MAX_SECRET_BYTES
  ) {
    throw new RangeError("Secret exceeds 65536 bytes");
  }
  return body;
}

export async function PUT(
  request: NextRequest,
  { params }: { params: Promise<{ name: string }> },
) {
  if (!getAllowWrites()) {
    return NextResponse.json(
      { error: "Dashboard is read-only" },
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
      { error: "A non-empty dashboard namespace is required for secret management" },
      { status: 400 },
    );
  }
  const url = namespacePath(selected.namespace, name);
  try {
    const body = await readSecretRequest(request);
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
  } catch (error) {
    if (error instanceof RangeError) {
      return NextResponse.json({ error: error.message }, { status: 413 });
    }
    if (error instanceof SyntaxError) {
      return NextResponse.json({ error: "Invalid request body" }, { status: 400 });
    }
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
      { error: "Dashboard is read-only" },
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
      { error: "A non-empty dashboard namespace is required for secret management" },
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
