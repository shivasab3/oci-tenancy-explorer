# AGENTS.md

## Start Here

This repository is a lightweight OCI operations portal. The browser stays static, the Python collectors talk to OCI, and local JSON snapshots power the UI.

If you are using Codex in this repo, read this file first, then skim `README.md`.

## What The App Does Today

The current app supports these surfaces:

- `Tenancy Overview`
- `Fleet View`
- `Shape Explorer`
- `Opportunities`
- `Console Announcements`
- `Asset Inspector`
- `Sync Data`

The product is designed around one tenancy at a time. OCI credentials stay on the machine running the collectors and are never exposed to browser JavaScript.

## Core Files

- `index.html`
  The entire frontend UI, tab system, feature-toggle handling, refresh modal, and snapshot rendering logic live here.
- `portal_server.py`
  Serves the app locally, exposes refresh/status APIs, supports branch-prefixed URLs, and handles Asset Inspector lookups.
- `build_fleet_data.py`
  Builds `fleet_data.json` for Fleet View and Tenancy Overview.
- `build_shape_data.py`
  Builds `fleet_data_shapes.json` for Shape Explorer.
- `build_opportunities_data.py`
  Builds `fleet_data_opportunities.json` for Opportunities.
- `build_announcements_data.py`
  Builds `fleet_data_announcements.json` for Console Announcements.
- `app_config.json`
  Controls feature visibility and which steps are included in `Sync Data`.
- `fleet_data_sample_json/`
  Sanitized sample JSON files for every supported snapshot type.

## How To Run Locally

From the repo root:

```bash
python3 -m pip install oci
python3 portal_server.py --profile DEFAULT
```

Open the app at:

```text
http://127.0.0.1:8765/index.html
```

Optional: the server also supports a branch-slug URL such as:

```text
http://127.0.0.1:8765/staging/index.html
```

URL behavior:

- The server detects a branch/folder slug and can serve a prefixed route like `/staging/index.html`.
- In that example, `staging` is the detected branch or folder slug.
- The branch-slug URL is only a convenience for making the active repo copy obvious in the browser.
- The root route still works as a fallback.
- Users do not have to include the branch slug in the URL.
- Use `portal_server.py`, not `file://` or a different static server, because the UI depends on local API endpoints for refresh and lookup behavior.

## OCI Authentication

The collectors and server support two auth modes:

- Local OCI config:

```bash
python3 portal_server.py --profile DEFAULT --config-file ~/.oci/config
```

- OCI instance principal:

```bash
python3 portal_server.py --auth instance_principal
```

The same auth pattern applies to the individual collectors.

Example local config:

```ini
[DEFAULT]
user=ocid1.user.oc1..example
fingerprint=aa:bb:cc:dd:ee:ff:11:22:33:44:55:66:77:88:99:00
tenancy=ocid1.tenancy.oc1..example
region=us-ashburn-1
key_file=/Users/you/.oci/oci_api_key.pem
```

Use `oci_config.example` as the starter reference. Never commit real OCI config or private keys.

## Snapshot Model

This app uses a local snapshot workflow:

1. A Python collector calls OCI public APIs through the OCI Python SDK.
2. The collector writes a local JSON file in the repo root.
3. `index.html` fetches that JSON and renders the view.

Current runtime snapshot files:

- `fleet_data.json`
- `fleet_data_shapes.json`
- `fleet_data_opportunities.json`
- `fleet_data_announcements.json`

Tracked examples live here instead:

- `fleet_data_sample_json/fleet_data.sample.json`
- `fleet_data_sample_json/fleet_data_shapes.sample.json`
- `fleet_data_sample_json/fleet_data_opportunities.sample.json`
- `fleet_data_sample_json/fleet_data_announcements.sample.json`

Do not commit fresh runtime tenancy snapshots unless the user explicitly asks for that.

## Collectors And OCI Services

### `build_fleet_data.py`

Purpose:

- Builds the main fleet snapshot used by Fleet View and Tenancy Overview.

Current OCI coverage:

- Identity
- Compute instances
- Compute maintenance events
- OS Management Hub boot data

Important behavior:

- Scans subscribed regions and accessible compartments.
- Logs region progress with `__OTX_EVENT__` messages for the refresh UI.
- Tries `list_instance_maintenance_events` by compartment.
- Falls back to instance summary maintenance data when that enrichment is unavailable or empty.
- Supports grouping customers by `tenancy`, `compartment`, or `tag`.

Example:

```bash
python3 build_fleet_data.py --profile DEFAULT --output fleet_data.json
```

### `build_shape_data.py`

Purpose:

- Builds a shape inventory snapshot from compute instances.

Current OCI coverage:

- Identity
- Compute instances

Output includes:

- `generatedAt`
- `source`
- `summary`
- `instances`
- `seriesSummary`
- `shapeSummary`
- `regionSummary`
- `stateSummary`
- `warnings`

Example:

```bash
python3 build_shape_data.py --profile DEFAULT --output fleet_data_shapes.json
```

### `build_opportunities_data.py`

Purpose:

- Builds evidence-backed maturity and opportunity signals across the tenancy.

Current OCI coverage includes:

- Identity
- Resource Search
- Budgets and Cost Analysis
- Cloud Guard
- Monitoring
- Logging
- OKE
- Bastion
- Vulnerability Scanning
- Service Connector Hub
- Auto Scaling
- Database
- APM
- Operations Insights
- ADM
- Load Balancers
- Network Load Balancers

Output is an analysis snapshot, not a raw inventory dump.

Example:

```bash
python3 build_opportunities_data.py --profile DEFAULT --output fleet_data_opportunities.json
```

### `build_announcements_data.py`

Purpose:

- Builds a local OCI Console Announcements snapshot.

Current OCI coverage:

- Identity
- Announcements Service

Important behavior:

- Queries announcements from the tenancy root compartment.
- Requests only the latest visible announcement in each chain.
- Uses best-effort detail enrichment when the API allows it.
- Writes warnings when detail lookups are not accessible.

Example:

```bash
python3 build_announcements_data.py --profile DEFAULT --output fleet_data_announcements.json
```

## `app_config.json`

Experimental feature visibility is controlled here:

```json
{
  "experimental-features": {
    "overview": true,
    "shapes": true,
    "opportunities": true,
    "announcements": true
  }
}
```

Current supported keys:

- `overview`
- `shapes`
- `opportunities`
- `announcements`

What the toggles currently affect:

- tab visibility in the UI
- About modal feature descriptions
- individual refresh availability where applicable
- `Sync Data` step selection

Compatibility note:

- Frontend and backend prefer `experimental-features`
- Both still accept the older `features` key as a fallback

Code defaults if the file is missing:

- `overview`: `false`
- `shapes`: `true`
- `opportunities`: `true`
- `announcements`: `true`

## UI Mental Model

The UI is mostly a single-file app in `index.html`.

Important patterns:

- top-level tabs are switched through `setPortalView(...)`
- feature gating is handled by `loadAppConfig()`, `isFeatureEnabled()`, and `getAvailablePortalViews()`
- refresh console modes are configured in `getRefreshModeConfig()`
- unified sync step order is fleet, shapes, opportunities, then announcements
- the combined collector log is shown in the right half of the Sync modal
- Asset Inspector uses `GET /api/resource/lookup?ocid=...`

Snapshot fetches are local relative files:

- `./fleet_data.json`
- `./fleet_data_shapes.json`
- `./fleet_data_opportunities.json`
- `./fleet_data_announcements.json`

## Adding A New Capability

Use this pattern for new tabs or new collectors:

1. Create a collector script like `build_<capability>_data.py`.
   It should use the same auth conventions, emit `__OTX_EVENT__` progress logs when useful, and write a stable JSON payload with `generatedAt`, `source`, and the capability-specific data.
2. Add server wiring in `portal_server.py`.
   Add the script path, output path, refresh runner, status endpoint, refresh endpoint, and optional unified sync step.
3. Add UI wiring in `index.html`.
   Add the tab button, view container, snapshot fetch logic, render function, refresh console metadata, and JSON source link if appropriate.
4. Decide whether the capability should be feature-gated.
   If yes, add a key in `app_config.json`, apply it to tab visibility, About modal content, and `Sync Data`.
5. Add a sanitized sample file in `fleet_data_sample_json/`.
6. Update docs.
   Usually `README.md`, `AGENTS.md`, and sometimes `SECURITY.md`.

## Sync Data Notes

`Sync Data` is the best end-to-end workflow for testing the whole app.

Current behavior:

- runs the enabled collectors sequentially
- shows per-step and per-region progress
- keeps running after non-fleet failures
- stops later steps if the fleet step fails
- respects `app_config.json` for optional capabilities

If a user says a tab is missing fresh data, the first thing to check is the combined collector log in the Sync modal.

## Troubleshooting

If the browser looks wrong:

- make sure you are serving this repo’s `portal_server.py`
- confirm the URL is pointing at the right repo slug such as `/staging/index.html`
- kill old local server processes before launching a new one if another repo was already serving the same port

If a collector runs but data is missing:

- check the refresh log first
- confirm the feature is enabled in `app_config.json`
- confirm the matching `build_*.py` file exists in this repo copy
- confirm OCI auth and tenancy access are valid for the compartments and services being queried

If maintenance dates are missing:

- the fleet collector already tries maintenance-event enrichment and then falls back to instance summary data
- refresh warnings are the first place to look for access, timeout, or service issues

## Safe Working Rules For Codex

- Work only in the repo copy the user names. If they say staging only, stay in staging only.
- Prefer `rg` for codebase search.
- Use `apply_patch` for manual edits.
- Do not revert unrelated user changes.
- Do not commit generated runtime JSON snapshots.
- Keep sanitized examples in `fleet_data_sample_json/`.
- Treat tenancy snapshot data as sensitive operational metadata.
- Prefer instance principals for shared deployments when practical.
