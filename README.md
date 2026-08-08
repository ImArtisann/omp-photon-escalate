# omp-photon-escalate

[![Plugin Demo](https://img.youtube.com/vi/U_tZUz47l20/maxresdefault.jpg)](https://youtu.be/U_tZUz47l20)

An [Oh My Pi](https://omp.sh/) extension that escalates unanswered `ask` tool
dialogs to iMessage through Photon's `spectrum-ts` SDK.

The normal terminal dialog opens first. If it remains unanswered for the
configured delay, the extension sends each question as text followed by a native
iMessage poll. A phone answer is returned to the agent in OMP's native `ask`
result shape.

## Features

- Preserves OMP's native `ask` schema, renderer, and result format.
- Supports single-choice and ordered multi-choice questions.
- Supports OMP's generated `Other…` option through a free-text iMessage reply.
- Requires a final `Submit` / `Start over` confirmation poll.
- Cancels the phone flow when the terminal answers first.
- Leaves the terminal dialog available if Photon is unavailable.
- Optionally keeps an away session active so the next ask escalates immediately.
- Uses the active OMP profile's configuration directory.

## Requirements

- OMP 17.2.11 or later.
- Bun.
- A Photon project with the iMessage provider enabled.
- The destination phone or iMessage email must be allowed by the Photon project.

Phone numbers must use E.164 format, for example `+15551234567`.

## Install

Install dependencies and register the package for the default OMP profile:

```bash
bun install
omp install . --scope=user
```

OMP installs the package under `~/.omp/plugins/node_modules/omp-photon-escalate`
and records it in `~/.omp/plugins/omp-plugins.lock.json`.

Create the default-profile configuration:

```bash
install -m 600 photon-escalate.example.json ~/.omp/agent/photon-escalate.json
```

Edit the installed configuration with the destination and Photon credentials.
See [Configuration](docs/configuration.md) for every setting and credential
options.

Verify installation:

```bash
omp plugin list --json
```

## Named profiles

OMP profiles isolate plugins, settings, sessions, and configuration. Install the
extension separately for each profile that needs it:

```bash
OMP_PROFILE=planning omp install . --scope=user
install -m 600 \
  ~/.omp/agent/photon-escalate.json \
  ~/.omp/profiles/planning/agent/photon-escalate.json
```

The extension resolves configuration through OMP's active agent directory, so
`--profile planning` reads the planning profile's copy rather than the default
profile's file.

## Timeout behavior

`escalateAfterSeconds` controls when Photon sends the poll. The default is 120
seconds.

Keep OMP's native timeout disabled:

```bash
omp config set ask.timeout 0
```

A nonzero `ask.timeout` can auto-answer the terminal dialog before Photon
escalation begins. It is not the Photon delay.

## How the race works

1. The extension opens OMP's native terminal ask through `ctx.invokeTool`.
2. A managed countdown starts in the same tool execution.
3. When the countdown expires, Photon sends the question text and poll.
4. Terminal and phone flows race; the winner aborts the other flow.
5. Phone answers are converted into OMP's native result details.

The countdown intentionally lives in the registered tool wrapper rather than a
legacy HookAPI module. A legacy `tool_call` hook runs before tool execution and
cannot delegate to, resolve, or safely cancel the pending native ask. Hooks are
appropriate for telemetry here, not flow ownership.

## Development

```bash
bun install
bunx tsc --noEmit
```

The extension factory is `src/main.ts`, declared by `omp.extensions` in
`package.json`.

The package includes Bun patches for Spectrum runtime compatibility.
`bun install` reapplies them from `patches/`.

## Security

Configuration may contain a Photon project secret and a personal destination.
Keep credential-bearing files out of version control and restrict them to the
current user:

```bash
chmod 600 ~/.omp/agent/photon-escalate.json
```

The project ignores `.env` and `photon-escalate.json`. Never commit either file.
