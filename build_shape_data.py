#!/usr/bin/env python3
"""
Build fleet_data_shapes.json from Oracle Cloud Infrastructure.

This collector focuses on OCI Compute shape inventory so the UI can provide a
tenancy-wide Shape Explorer without relying on ad hoc local CSV output.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import oci

from build_fleet_data import (
    emit_event,
    get_tenancy_id,
    iso_utc,
    list_call_get_all_results,
    list_compartments,
    list_regions,
    load_signer_and_config,
    log,
    require_oci_sdk,
    resolve_tenancy_name,
    utc_now,
)


DEFAULT_OUTPUT = "fleet_data_shapes.json"


@dataclass
class ShapeArgs:
    auth: str
    config_file: str
    profile: str
    output: Path
    max_region_workers: int


def parse_args() -> ShapeArgs:
    parser = argparse.ArgumentParser(description="Build OCI shape inventory from OCI APIs.")
    parser.add_argument("--auth", choices=["config", "instance_principal"], default="config")
    parser.add_argument("--config-file", default="~/.oci/config")
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--max-region-workers",
        type=int,
        default=3,
        help="Maximum number of regions to process in parallel.",
    )
    ns = parser.parse_args()
    return ShapeArgs(
        auth=ns.auth,
        config_file=ns.config_file,
        profile=ns.profile,
        output=Path(ns.output).expanduser(),
        max_region_workers=max(1, ns.max_region_workers),
    )


def build_compute_client(config: dict[str, Any], signer: Any, region: str):
    regional_config = dict(config)
    regional_config["region"] = region
    kwargs = {"config": regional_config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.core.ComputeClient(**kwargs)


def shape_series(shape: str) -> str:
    parts = str(shape or "").split(".")
    if len(parts) >= 3 and parts[2]:
        return parts[2].upper()
    return "UNKNOWN"


def collect_region_shapes(
    config: dict[str, Any],
    signer: Any,
    region: str,
    compartments: dict[str, dict[str, str]],
) -> tuple[list[dict[str, Any]], list[str]]:
    emit_event("region_started", region=region, phase="Starting", startedAt=iso_utc(utc_now()))
    emit_event("region_phase", region=region, phase="Collecting Compute Shapes", status="running")
    log(f"Scanning shape inventory in {region}")

    client = build_compute_client(config, signer, region)
    rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    for compartment_id, compartment_meta in compartments.items():
        compartment_name = compartment_meta.get("name") or compartment_id
        log(f"Listing compute instances in {region} for compartment {compartment_name}")
        try:
            response = list_call_get_all_results(client.list_instances, compartment_id=compartment_id)
            instances = [] if response is None or getattr(response, "data", None) is None else response.data
            for instance in instances:
                rows.append(
                    {
                        "region": region,
                        "availabilityDomain": instance.availability_domain or "",
                        "faultDomain": instance.fault_domain or "",
                        "shapeSeries": shape_series(instance.shape or ""),
                        "shape": instance.shape or "",
                        "instanceName": instance.display_name or "",
                        "instanceOcid": instance.id or "",
                        "instanceState": instance.lifecycle_state or "",
                        "compartmentName": compartment_name,
                        "compartmentOcid": compartment_id,
                        "dedicatedVmHostOcid": getattr(instance, "dedicated_vm_host_id", "") or "",
                    }
                )
        except Exception as exc:  # pragma: no cover - tenancy-wide collector should keep going
            warning = f"Shape listing failed in {region} for compartment {compartment_name}: {exc}"
            warnings.append(warning)
            log(warning)

    emit_event("region_phase", region=region, phase="Building Shape Summary", status="running")
    emit_event(
        "region_completed",
        region=region,
        finishedAt=iso_utc(utc_now()),
        instanceCount=len(rows),
    )
    log(f"Region {region} contributed {len(rows)} shape row(s)")
    return rows, warnings


def build_series_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("shapeSeries") or "UNKNOWN"].append(row)
    summary = []
    for series, items in sorted(grouped.items()):
        summary.append(
            {
                "shapeSeries": series,
                "instanceCount": len(items),
                "shapeCount": len({item.get("shape") for item in items if item.get("shape")}),
                "regionCount": len({item.get("region") for item in items if item.get("region")}),
                "dedicatedHostPlacementCount": sum(1 for item in items if item.get("dedicatedVmHostOcid")),
            }
        )
    return summary


def build_shape_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[row.get("shape") or "Unknown Shape"].append(row)
    summary = []
    for shape_name, items in sorted(grouped.items()):
        summary.append(
            {
                "shape": shape_name,
                "shapeSeries": items[0].get("shapeSeries") or "UNKNOWN",
                "instanceCount": len(items),
                "regionCount": len({item.get("region") for item in items if item.get("region")}),
                "compartmentCount": len({item.get("compartmentName") for item in items if item.get("compartmentName")}),
            }
        )
    summary.sort(key=lambda item: (-int(item["instanceCount"]), item["shape"]))
    return summary


def build_region_summary(rows: list[dict[str, Any]], regions: list[str]) -> list[dict[str, Any]]:
    region_rows: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        region_rows[row.get("region") or ""].append(row)

    summary = []
    for region in regions:
        items = region_rows.get(region, [])
        series_counter = Counter(item.get("shapeSeries") or "UNKNOWN" for item in items)
        summary.append(
            {
                "region": region,
                "instanceCount": len(items),
                "shapeCount": len({item.get("shape") for item in items if item.get("shape")}),
                "seriesBreakdown": [
                    {"shapeSeries": series, "instanceCount": count}
                    for series, count in sorted(series_counter.items(), key=lambda item: (-item[1], item[0]))
                ],
            }
        )
    return summary


def build_state_summary(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    counter = Counter((row.get("instanceState") or "UNKNOWN").upper() for row in rows)
    return [
        {"instanceState": state, "instanceCount": count}
        for state, count in sorted(counter.items(), key=lambda item: (-item[1], item[0]))
    ]


def main() -> int:
    require_oci_sdk()
    args = parse_args()
    config, signer = load_signer_and_config(args)
    generated_at = utc_now()

    tenancy_id = get_tenancy_id(config, signer)
    identity_kwargs = {"config": config}
    if signer is not None:
        identity_kwargs["signer"] = signer
    identity_client = oci.identity.IdentityClient(**identity_kwargs)
    tenancy_name = resolve_tenancy_name(identity_client, tenancy_id)
    compartments = list_compartments(identity_client, tenancy_id)
    regions = list_regions(identity_client, tenancy_id)

    log(f"Starting OCI shape build with profile '{args.profile if args.auth == 'config' else 'instance_principal'}'")
    log(f"Connected to tenancy: {tenancy_name} ({tenancy_id})")
    log(f"Loaded {len(compartments)} accessible compartments")
    log(f"Scanning subscribed regions for shape inventory: {', '.join(regions)}")
    emit_event("regions_discovered", regions=regions)

    all_rows: list[dict[str, Any]] = []
    warnings: list[str] = []

    with concurrent.futures.ThreadPoolExecutor(max_workers=args.max_region_workers) as executor:
        futures = [
            executor.submit(collect_region_shapes, config, signer, region, compartments)
            for region in regions
        ]
        for future in concurrent.futures.as_completed(futures):
            region_rows, region_warnings = future.result()
            all_rows.extend(region_rows)
            warnings.extend(region_warnings)

    all_rows.sort(
        key=lambda row: (
            row.get("region") or "",
            row.get("availabilityDomain") or "",
            row.get("faultDomain") or "",
            row.get("shapeSeries") or "",
            row.get("shape") or "",
            row.get("instanceName") or "",
            row.get("instanceOcid") or "",
        )
    )

    payload = {
        "generatedAt": iso_utc(generated_at),
        "source": {
            "tenancyName": tenancy_name,
            "tenancyId": tenancy_id,
            "regions": regions,
            "compartmentCount": len(compartments),
        },
        "summary": {
            "instanceCount": len(all_rows),
            "regionCount": len({row.get("region") for row in all_rows if row.get("region")}),
            "shapeSeriesCount": len({row.get("shapeSeries") for row in all_rows if row.get("shapeSeries")}),
            "shapeCount": len({row.get("shape") for row in all_rows if row.get("shape")}),
            "dedicatedHostPlacementCount": sum(1 for row in all_rows if row.get("dedicatedVmHostOcid")),
        },
        "instances": all_rows,
        "seriesSummary": build_series_summary(all_rows),
        "shapeSummary": build_shape_summary(all_rows),
        "regionSummary": build_region_summary(all_rows, regions),
        "stateSummary": build_state_summary(all_rows),
        "warnings": warnings,
    }

    args.output.write_text(json.dumps(payload, indent=2))
    log(f"Wrote OCI shape inventory to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
