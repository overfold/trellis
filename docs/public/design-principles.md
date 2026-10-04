# Design principles

These principles decide what belongs in Trellis and what does not. Use them to judge whether a feature fits and how to weigh implementation tradeoffs. The [README](../../README.md#design-principles) summarizes them.

**Modular and extensible, but focused.** Trellis exposes clean primitives you can build on. The core repository stays lean — only the essentials live here. If you need something specific to your environment, you can extend Trellis yourself rather than waiting on a plugin ecosystem. No Kubernetes complexity, and no surface area you did not ask for.

**Non-opinionated and flexible.** Trellis provides the necessary building blocks without prescribing application architecture. Reverse proxies, for instance, are ordinary jobs rather than a special first-class service or ingress resource. Namespaces separate resources, discovery, and workload networks; API credentials grant cluster-wide access, and namespaces are not admission policy for arbitrary job manifests. Trellis does not add separate team or project abstractions on top. Operators can run Trellis as-is, or build their own frontends and abstractions on top for their specific use case. See [Multitenancy and trust boundaries](multitenancy.md) when building for untrusted tenants.

**Consumers own representation; Trellis owns meaning.** The control-plane API consumes canonical JSON, not YAML, HCL, Python, or another authoring language. A consumer may expose any representation it wants, but it must convert that representation into the canonical JSON model before calling Trellis. Human conveniences such as `64MiB` or `10s` therefore belong to the consumer; canonical validation, defaults, planning, revision semantics, and reconciliation belong to Trellis. This keeps custom frontends and abstractions open-ended without allowing each interface to invent different Trellis semantics.

**Declarative, with open-ended delivery.** Trellis accepts declarative desired state. The first-party human-authored representation is YAML, but it is only one consumer of the canonical JSON model. You can use `trellisctl`, drive it from CI/CD, build a custom UI or HCL/Python abstraction, or integrate with any tooling that can produce the API model. The workflow that generates and submits desired state is entirely yours.

**Easy to use.** The tension between "flexible building blocks" and "easy to use" is addressed through thorough documentation and first-party examples. Trellis favors clear documentation over opinionated defaults that hide what is actually happening.

**Open-source.** Trellis is fully open-source. Read it, modify it, and run it wherever you like.

[Documentation index](../README.md)
