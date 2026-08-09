# Configuration

`omp-photon-escalate` reads one JSON configuration file for each `ask` call.
This allows changes to take effect without restarting OMP.

## File resolution

The first existing path wins:

1. The absolute path in `OMP_PHOTON_ESCALATE_CONFIG`.
2. `<working-directory>/.omp/photon-escalate.json`.
3. `<active-agent-directory>/photon-escalate.json`.

The active agent directory is profile-aware:

| OMP profile                       | Default configuration path                            |
| --------------------------------- | ----------------------------------------------------- |
| Default                           | `~/.omp/agent/photon-escalate.json`                   |
| Named profile, such as `planning` | `~/.omp/profiles/planning/agent/photon-escalate.json` |

A project configuration therefore overrides the active profile's user
configuration. `OMP_PHOTON_ESCALATE_CONFIG` must be absolute; a relative value
is ignored.

## Complete example

```json
{
    "enabled": true,
    "escalateAfterSeconds": 120,
    "nudgeIntervalSeconds": 1800,
    "stickyAwayMode": true,
    "phone": "+15551234567",
    "line": "+15559876543",
    "projectId": "your-photon-project-id",
    "projectSecret": "your-photon-project-secret",
    "labelPrefix": "omp"
}
```

Start from the included template:

```bash
install -m 600 photon-escalate.example.json ~/.omp/agent/photon-escalate.json
```

## Settings

| Setting                | Type    | Default | Required | Description                                                                                                                        |
| ---------------------- | ------- | ------- | -------- | ---------------------------------------------------------------------------------------------------------------------------------- |
| `enabled`              | boolean | `true`  | No       | Enables Photon escalation. When false, the extension delegates directly to native `ask`.                                           |
| `escalateAfterSeconds` | number  | `120`   | No       | Seconds to wait before connecting to Photon and sending the first question. Must be non-negative. `0` escalates immediately.       |
| `nudgeIntervalSeconds` | number  | `1800`  | No       | Seconds between reminder messages while waiting for a phone answer. `0` disables nudges.                                           |
| `stickyAwayMode`       | boolean | `true`  | No       | After a phone-completed ask, escalate subsequent asks immediately for the rest of the session. A terminal answer clears away mode. |
| `phone`                | string  | —       | Yes      | Destination iMessage handle. Use an E.164 phone number such as `+15551234567`, or an iMessage email address.                       |
| `line`                 | string  | —       | No       | Specific outbound iMessage line to use when the Photon project exposes more than one. Use E.164 format when it is a phone number.  |
| `projectId`            | string  | —       | Yes*     | Photon project ID. May instead come from `SPECTRUM_PROJECT_ID`.                                                                    |
| `projectSecret`        | string  | —       | Yes*     | Photon project secret. May instead come from `SPECTRUM_PROJECT_SECRET`.                                                            |
| `labelPrefix`          | string  | `"omp"` | No       | Non-empty prefix used in poll titles and response correlation.                                                                     |

`projectId` and `projectSecret` are required after environment-variable fallback
is applied.

## Credential options

### Store credentials in the JSON file

This is straightforward for a profile-specific installation. Protect the file:

```bash
chmod 600 ~/.omp/agent/photon-escalate.json
chmod 600 ~/.omp/profiles/planning/agent/photon-escalate.json
```

### Supply credentials through the environment

Omit `projectId` and `projectSecret` from JSON and provide:

```bash
export SPECTRUM_PROJECT_ID='your-project-id'
export SPECTRUM_PROJECT_SECRET='your-project-secret'
```

These variables must be present in the environment of the OMP process. A
project-local `.env` is only suitable when OMP is launched from that project and
the runtime loads it; it is not a substitute for profile-wide configuration.

## OMP native timeout

Photon escalation and OMP's native timeout are separate controls:

- `escalateAfterSeconds`: when to send the iMessage poll.
- `ask.timeout`: when OMP auto-selects a terminal answer.

Keep the native timeout at zero:

```bash
omp config set ask.timeout 0
omp config get ask.timeout
```

Expected output:

```text
0
```

If `ask.timeout` is nonzero, the native dialog may resolve before the Photon
countdown. The extension warns at session startup when it detects this
condition.

## Configure a named profile

Install the plugin and configuration independently because OMP profiles isolate
plugin registries and agent state:

```bash
OMP_PROFILE=planning omp install github:ImArtisann/omp-photon-escalate --scope=user
install -m 600 \
  ~/.omp/agent/photon-escalate.json \
  ~/.omp/profiles/planning/agent/photon-escalate.json
```

Verify the profile registry:

```bash
OMP_PROFILE=planning omp plugin list --json
```

Update the copied profile file independently if the planning profile should use
a different destination, Photon project, delay, or nudge interval.

## Project override

For a repository-specific destination or delay:

```bash
mkdir -p .omp
install -m 600 photon-escalate.example.json .omp/photon-escalate.json
```

This file takes precedence over the active profile's configuration unless
`OMP_PHOTON_ESCALATE_CONFIG` points to another existing file.

Ensure the repository ignores `.omp/photon-escalate.json` before placing
credentials there.

## Failure behavior

- Missing or invalid configuration produces one warning per session and leaves
  native terminal `ask` available.
- A Photon connection or send failure is logged and shown as a warning; the
  terminal dialog remains active.
- If the terminal answers after a poll was sent, the extension cancels phone
  waiting and sends an “Answered in the terminal” notice to the conversation.
- Configuration is validated before Photon connection. Numeric delays must be
  finite and non-negative, and string fields must not be blank.

## Troubleshooting

### The poll never arrives

1. Confirm the plugin is enabled with `omp plugin list --json` or
   `OMP_PROFILE=<name> omp plugin list --json`.
2. Confirm the active profile has `photon-escalate.json` in its own agent
   directory.
3. Confirm `phone` is E.164, including the leading `+` and country code.
4. Confirm the destination is allowed by the Photon project.
5. Confirm `ask.timeout` is `0`.
6. Check OMP logs for `photon-escalate: phone flow failed`.

### The wrong profile configuration is used

Check how OMP was launched:

```bash
omp --profile planning
# or
OMP_PROFILE=planning omp
```

A named profile reads `~/.omp/profiles/<name>/agent/photon-escalate.json`. It
does not inherit the default profile's plugin installation or configuration
automatically.

### Test without waiting

Temporarily set:

```json
{
    "escalateAfterSeconds": 0
}
```

Retain the other required fields, start a fresh OMP session, and invoke `ask`.
Restore the intended delay after the smoke test.
