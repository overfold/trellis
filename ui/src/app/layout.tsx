export const dynamic = "force-dynamic";

import type { Metadata } from "next";
import { Geist, Geist_Mono } from "next/font/google";
import { Sidebar } from "@/components/sidebar";
import { ConfigProvider } from "@/components/config-provider";
import {
  getAllowWrites,
  getClusterName,
  getConfiguredNamespaces,
  getDefaultNamespace,
  hasConfiguredNamespaceAllowlist,
} from "@/lib/orchestrator";
import "./globals.css";

const geistSans = Geist({
  variable: "--font-geist-sans",
  subsets: ["latin"],
});

const geistMono = Geist_Mono({
  variable: "--font-geist-mono",
  subsets: ["latin"],
});

export const metadata: Metadata = {
  title: "Trellis Console",
  description: "Low-level operator console for Trellis",
};

export default function RootLayout({
  children,
}: {
  children: React.ReactNode;
}) {
  const allowWrites = getAllowWrites();
  const clusterName = getClusterName();
  const namespaces = getConfiguredNamespaces();
  const defaultNamespace = getDefaultNamespace();
  const allowAnyNamespace = !hasConfiguredNamespaceAllowlist();
  return (
    <html
      lang="en"
      className={`${geistSans.variable} ${geistMono.variable} h-full antialiased`}
    >
      <body className="flex h-full bg-background text-foreground">
        <ConfigProvider
          allowWrites={allowWrites}
          clusterName={clusterName}
          defaultNamespace={defaultNamespace}
          namespaces={namespaces}
          allowAnyNamespace={allowAnyNamespace}
        >
          <Sidebar />
          <main className="flex-1 overflow-y-auto">
            <div className="mx-auto max-w-5xl px-6 py-8">{children}</div>
          </main>
        </ConfigProvider>
      </body>
    </html>
  );
}
