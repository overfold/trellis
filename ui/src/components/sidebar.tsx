"use client";

import Link from "next/link";
import { usePathname, useRouter } from "next/navigation";
import { cn } from "@/lib/utils";
import { useOrchestratorStatus } from "@/hooks/use-api";
import { useConfig } from "./config-provider";

const navigation = [
  {
    name: "Operations",
    href: "/",
    icon: (
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <rect x="1.5" y="1.5" width="6" height="6" rx="1" />
        <rect x="10.5" y="1.5" width="6" height="6" rx="1" />
        <rect x="1.5" y="10.5" width="6" height="6" rx="1" />
        <rect x="10.5" y="10.5" width="6" height="6" rx="1" />
      </svg>
    ),
  },
  {
    name: "Nodes",
    href: "/nodes",
    clusterOnly: true,
    icon: (
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <rect x="3" y="1.5" width="12" height="5" rx="1" />
        <rect x="3" y="11.5" width="12" height="5" rx="1" />
        <path d="M9 6.5v5" />
      </svg>
    ),
  },
  {
    name: "Jobs",
    href: "/jobs",
    icon: (
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M3 5h12M3 9h12M3 13h8" />
      </svg>
    ),
  },
  {
    name: "Secrets",
    href: "/secrets",
    clusterOnly: true,
    icon: (
      <svg width="18" height="18" viewBox="0 0 18 18" fill="none" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="6" cy="9" r="3" />
        <path d="M9 9h7M13 9v2M15 9v2" />
      </svg>
    ),
  },
];

export function Sidebar() {
  const pathname = usePathname();
  const router = useRouter();
  const cluster = useOrchestratorStatus();
  const {
    allowWrites,
    apiAccess,
    clusterName,
    namespace,
    namespaces,
    setNamespace,
  } = useConfig();

  const visibleNavigation = navigation.filter(
    (item) => !item.clusterOnly || apiAccess === "cluster",
  );

  return (
    <aside className="flex h-full w-56 shrink-0 flex-col border-r border-border bg-card">
      <div className="flex h-14 items-center gap-2.5 border-b border-border px-5">
        <div className="flex h-7 w-7 items-center justify-center rounded-md bg-emerald-600 text-white">
          <svg width="14" height="14" viewBox="0 0 14 14" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <path d="M2 7h10M7 2v10M2 2l10 10M12 2L2 12" />
          </svg>
        </div>
        <div className="leading-tight">
          <span className="block text-[15px] font-semibold tracking-tight text-foreground">Trellis</span>
          <span className="block text-[10px] font-medium uppercase tracking-wider text-muted-foreground">Console</span>
        </div>
      </div>

      <nav className="flex flex-1 flex-col gap-0.5 p-3">
        {visibleNavigation.map((item) => {
          const isActive = item.href === "/" ? pathname === "/" : pathname.startsWith(item.href);
          return (
            <Link
              key={item.name}
              href={item.href}
              className={cn(
                "flex items-center gap-3 rounded-md px-3 py-2 text-sm font-medium transition-colors",
                isActive
                  ? "bg-accent text-accent-foreground"
                  : "text-muted-foreground hover:bg-accent hover:text-accent-foreground",
              )}
            >
              {item.icon}
              {item.name}
            </Link>
          );
        })}
      </nav>

      <div className="space-y-4 border-t border-border p-4">
        <div>
          <div>
            <p className="text-[10px] font-medium uppercase tracking-wider text-muted-foreground">Cluster</p>
            <div className="mt-1.5 flex items-center gap-2">
              <span
                aria-label={cluster.connected ? "Connected" : cluster.loading ? "Connecting" : "Unavailable"}
                className={`h-2 w-2 shrink-0 rounded-full ${
                  cluster.connected
                    ? "bg-emerald-500"
                    : cluster.loading
                      ? "bg-zinc-400"
                      : "bg-red-500"
                }`}
              />
              <p className="truncate font-mono text-xs font-medium text-foreground" title={clusterName}>
                {clusterName}
              </p>
            </div>
          </div>
          <p className="mt-1.5 text-xs text-muted-foreground">
            {allowWrites ? "Read-write mode" : "Read-only mode"}
          </p>
        </div>
        <div>
          <label
            htmlFor="namespace-context"
            className="block text-[10px] font-medium uppercase tracking-wider text-muted-foreground"
          >
            Namespace
          </label>
          {apiAccess === "cluster" ? (
            <select
              id="namespace-context"
              value={namespace}
              onChange={(event) => {
                setNamespace(event.target.value);
                router.push("/");
              }}
              className="mt-1.5 w-full rounded-md border border-border bg-card px-2 py-1.5 font-mono text-xs text-foreground outline-none focus:ring-2 focus:ring-emerald-500/40"
            >
              <option value="">All namespaces</option>
              {namespaces.map((item) => (
                item && <option key={item} value={item}>{item}</option>
              ))}
            </select>
          ) : (
            <p className="mt-1.5 truncate rounded-md border border-border bg-background px-2 py-1.5 font-mono text-xs text-foreground" title={namespace || "unscoped"}>
              {namespace || "unscoped"}
            </p>
          )}
        </div>
      </div>
    </aside>
  );
}
