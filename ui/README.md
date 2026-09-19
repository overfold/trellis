# Trellis Console

The `ui/` directory contains Trellis Console, the first-party low-level Next.js operator client for Trellis. It is a graphical equivalent of `trellisctl`: inspect jobs, revisions, allocations, nodes, secrets metadata, and canonical API JSON without adding application-platform abstractions.

Operator-facing behavior, authorization, configuration, manifest editing, production deployment, and troubleshooting are documented in the authoritative [Trellis Console guide](../docs/public/dashboard.md). Keep those details there rather than duplicating them in this package README.

## Local development

Requires Node.js 20 or later and npm.

```sh
cp .env.example .env.local
npm ci
npm run dev
```

Open <http://localhost:3000>. Source lives under `src/app`, shared UI pieces under `src/components`, browser data hooks under `src/hooks`, and Trellis API/manifest helpers under `src/lib`.

The browser talks only to same-origin Next.js routes under `/api/v1`; those server-side handlers forward requests to the configured Trellis API and attach its bearer credential. Do not expose Trellis credentials through `NEXT_PUBLIC_` variables.

## Quality checks

```sh
npm run lint
npm run build
```

## Production build

```sh
npm ci
npm run build
npm run start
```

The repository also provides a `Dockerfile` and `start.sh` for the released Console image. See the [Trellis Console guide](../docs/public/dashboard.md) for supported runtime configuration and deployment guidance.
