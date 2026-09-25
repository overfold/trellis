"use client";

import { createContext, useContext, useEffect, useState } from "react";

type DashboardAPIAccess = "namespace" | "cluster";
type DashboardAccessLevel = "read" | "write";

const namespacePattern = /^[A-Za-z0-9][A-Za-z0-9_.-]{0,62}$/;

interface Config {
  allowWrites: boolean;
  apiAccess: DashboardAPIAccess;
  accessLevel: DashboardAccessLevel;
  clusterName: string;
  namespace: string;
  namespaces: string[];
  allowAnyNamespace: boolean;
  setNamespace: (namespace: string) => void;
}

const ConfigContext = createContext<Config>({
  allowWrites: false,
  apiAccess: "namespace",
  accessLevel: "read",
  clusterName: "Trellis cluster",
  namespace: "",
  namespaces: [""],
  allowAnyNamespace: false,
  setNamespace: () => undefined,
});

export function useConfig(): Config {
  return useContext(ConfigContext);
}

export function ConfigProvider({
  allowWrites,
  clusterName,
  defaultNamespace,
  namespaces,
  allowAnyNamespace,
  children,
}: {
  allowWrites: boolean;
  clusterName: string;
  defaultNamespace: string;
  namespaces: string[];
  allowAnyNamespace: boolean;
  children: React.ReactNode;
}) {
  const [apiAccess, setAPIAccess] = useState<DashboardAPIAccess>("namespace");
  const [accessLevel, setAccessLevel] = useState<DashboardAccessLevel>("read");
  const [namespace, setSelectedNamespace] = useState(defaultNamespace);
  const [knownNamespaces, setKnownNamespaces] = useState(namespaces);

  useEffect(() => {
    let cancelled = false;

    const loadCredential = async () => {
      try {
        const response = await fetch("/api/v1/access", { cache: "no-store" });
        if (!response.ok) return;
        const data = (await response.json()) as {
          scope?: DashboardAPIAccess;
          access?: DashboardAccessLevel;
          namespace?: string;
        };
        if (
          cancelled ||
          (data.scope !== "cluster" && data.scope !== "namespace") ||
          (data.access !== "read" && data.access !== "write")
        ) {
          return;
        }
        setAPIAccess(data.scope);
        setAccessLevel(data.access);
        if (
          data.scope === "namespace" &&
          data.namespace &&
          namespacePattern.test(data.namespace)
        ) {
          setSelectedNamespace(data.namespace);
          setKnownNamespaces([data.namespace]);
          return;
        }
        if (data.scope === "cluster" && allowAnyNamespace) {
          const namespaceResponse = await fetch("/api/v1/namespaces", {
            cache: "no-store",
          });
          if (!namespaceResponse.ok || cancelled) return;
          const discovered = (await namespaceResponse.json()) as unknown;
          if (!Array.isArray(discovered)) return;
          const valid = discovered.filter(
            (value): value is string =>
              typeof value === "string" && namespacePattern.test(value),
          );
          const merged = Array.from(new Set([...namespaces.filter(Boolean), ...valid])).sort();
          setKnownNamespaces(merged.length > 0 ? merged : namespaces);
          if (!namespace && merged.length > 0) {
            setSelectedNamespace(merged[0]);
          }
        }
      } catch {
        // Keep safe namespace/read defaults when credential introspection or discovery is unavailable.
      }
    };

    void loadCredential();
    return () => {
      cancelled = true;
    };
  }, [allowAnyNamespace, namespace, namespaces]);

  const setNamespace = (next: string) => {
    if (apiAccess !== "cluster") return;
    const candidate = next.trim();
    if (candidate === "") {
      setSelectedNamespace("");
      return;
    }
    if (!namespacePattern.test(candidate)) return;
    if (!allowAnyNamespace && !namespaces.includes(candidate)) return;
    setSelectedNamespace(candidate);
    if (allowAnyNamespace) {
      setKnownNamespaces((current) =>
        current.includes(candidate) ? current : [...current, candidate].sort(),
      );
    }
  };

  return (
    <ConfigContext.Provider
      value={{
        allowWrites: allowWrites && accessLevel === "write",
        apiAccess,
        accessLevel,
        clusterName,
        namespace,
        namespaces:
          allowAnyNamespace && apiAccess === "cluster" ? knownNamespaces : namespaces,
        allowAnyNamespace: allowAnyNamespace && apiAccess === "cluster",
        setNamespace,
      }}
    >
      {children}
    </ConfigContext.Provider>
  );
}
