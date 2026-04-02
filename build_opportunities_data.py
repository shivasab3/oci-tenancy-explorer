#!/usr/bin/env python3
"""
Build fleet_data_opportunities.json from Oracle Cloud Infrastructure.

This collector is intentionally separate from build_fleet_data.py so the main
fleet refresh can stay focused and fast while opportunity analysis can go
deeper across tenancy capabilities.
"""

from __future__ import annotations

import argparse
import concurrent.futures
import json
import threading
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Callable

from build_fleet_data import (
    UTC,
    list_instances_for_region,
    emit_event,
    get_tenancy_id,
    iso_utc,
    list_call_get_all_results,
    list_compartments,
    list_regions,
    load_signer_and_config,
    log,
    parse_any_datetime,
    require_oci_sdk,
    resolve_tenancy_name,
    utc_now,
)

import oci


DEFAULT_OUTPUT = "fleet_data_opportunities.json"
POSTGRES_KEYWORDS = ("postgres", "postgresql")
OCI_BLOG_HUB_URL = "https://blogs.oracle.com/cloud-infrastructure/"

CATEGORY_RESOURCE_LINKS: dict[str, list[dict[str, str]]] = {
    "Automation": [
        {"label": "Right-Size Your Workloads with OCI Autoscaling", "url": "https://blogs.oracle.com/cloud-infrastructure/right-size-your-workloads-with-oracle-cloud-infrastructure-autoscaling", "type": "Blog"},
        {"label": "OCI Autoscaling Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/AutoScaling/home.htm", "type": "Documentation"},
        {"label": "OCI Instance Pools Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Compute/Tasks/creatinginstancepool.htm", "type": "Documentation"},
    ],
    "Containers": [
        {"label": "OCI Container Image Scanning, Signing, and Verification", "url": "https://blogs.oracle.com/cloud-infrastructure/oracle-cloud-infrastructure-container-image-scanning-signing-and-verification", "type": "Blog"},
        {"label": "OKE Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/ContEng/home.htm", "type": "Documentation"},
        {"label": "OKE Cluster Autoscaler Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contengusingclusterautoscaler_topic-Working_with_Cluster_Autoscaler_as_Cluster_Add-on.htm", "type": "Documentation"},
    ],
    "Data Platform": [
        {"label": "Introducing OCI Database with PostgreSQL", "url": "https://blogs.oracle.com/cloud-infrastructure/oci-database-postgres", "type": "Blog"},
        {"label": "PostgreSQL Service Documentation", "url": "https://docs.oracle.com/en-us/iaas/postgresql/home.htm", "type": "Documentation"},
        {"label": "Operations Insights Documentation", "url": "https://docs.oracle.com/en-us/iaas/operations-insights/home.htm", "type": "Documentation"},
    ],
    "FinOps": [
        {"label": "Introducing OCI Cost Anomaly Detection", "url": "https://blogs.oracle.com/cloud-infrastructure/introducing-ocis-cost-anomaly-detection", "type": "Blog"},
        {"label": "OCI Budgets Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/budgets.htm", "type": "Documentation"},
        {"label": "Cost Analysis And Usage Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/costanalysisoverview.htm", "type": "Documentation"},
    ],
    "Observability": [
        {"label": "OpenTelemetry Instrumentation with OCI APM", "url": "https://blogs.oracle.com/cloud-infrastructure/opentelemetry-instrumentation-oci-apm", "type": "Blog"},
        {"label": "OCI Logging Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Logging/home.htm", "type": "Documentation"},
        {"label": "Application Performance Monitoring Documentation", "url": "https://docs.oracle.com/en-us/iaas/application-performance-monitoring/home.htm", "type": "Documentation"},
    ],
    "Security": [
        {"label": "Simplify Secure Access with OCI Bastion Service", "url": "https://blogs.oracle.com/cloud-infrastructure/simplify-secure-access-with-oci-bastion-service", "type": "Blog"},
        {"label": "Cloud Guard Documentation", "url": "https://docs.oracle.com/en-us/iaas/cloud-guard/using/about-cloud-guard.htm", "type": "Documentation"},
        {"label": "Vulnerability Scanning Documentation", "url": "https://docs.oracle.com/en-us/iaas/scanning/home.htm", "type": "Documentation"},
        {"label": "OCI Bastion Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Bastion/home.htm", "type": "Documentation"},
    ],
}

OPPORTUNITY_RESOURCE_LINKS: dict[str, list[dict[str, str]]] = {
    "bastion_gap": [
        {"label": "Simplify Secure Access with OCI Bastion Service", "url": "https://blogs.oracle.com/cloud-infrastructure/simplify-secure-access-with-oci-bastion-service", "type": "Blog"},
        {"label": "OCI Bastion Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Bastion/home.htm", "type": "Documentation"},
    ],
    "budgets_gap": [
        {"label": "Enforced Budgets on OCI using Functions and Quotas", "url": "https://blogs.oracle.com/cloud-infrastructure/enforced-budgets-on-oci-using-functions-and-quotas", "type": "Blog"},
        {"label": "OCI Budgets Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/budgets.htm", "type": "Documentation"},
    ],
    "cloud_guard_gap": [
        {"label": "Cloud Guard Documentation", "url": "https://docs.oracle.com/en-us/iaas/cloud-guard/using/about-cloud-guard.htm", "type": "Documentation"},
        {"label": "OCI Well-Architected Framework", "url": "https://blogs.oracle.com/cloud-infrastructure/oci-wellarchitected-framework", "type": "Blog"},
    ],
    "cost_anomaly_gap": [
        {"label": "Introducing OCI Cost Anomaly Detection", "url": "https://blogs.oracle.com/cloud-infrastructure/introducing-ocis-cost-anomaly-detection", "type": "Blog"},
        {"label": "OCI Budgets Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Billing/Concepts/budgets.htm", "type": "Documentation"},
    ],
    "managed_postgresql_gap": [
        {"label": "Introducing OCI Database with PostgreSQL", "url": "https://blogs.oracle.com/cloud-infrastructure/oci-database-postgres", "type": "Blog"},
        {"label": "PostgreSQL Service Documentation", "url": "https://docs.oracle.com/en-us/iaas/postgresql/home.htm", "type": "Documentation"},
    ],
    "observability_gap": [
        {"label": "OpenTelemetry Instrumentation with OCI APM", "url": "https://blogs.oracle.com/cloud-infrastructure/opentelemetry-instrumentation-oci-apm", "type": "Blog"},
        {"label": "OCI Logging Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/Logging/home.htm", "type": "Documentation"},
        {"label": "Application Performance Monitoring Documentation", "url": "https://docs.oracle.com/en-us/iaas/application-performance-monitoring/home.htm", "type": "Documentation"},
    ],
    "oke_elasticity_gap": [
        {"label": "Modernizing Heterogeneous Workloads on OCI", "url": "https://blogs.oracle.com/cloud-infrastructure/modernizing-heterogeneous-workloads-on-oci", "type": "Blog"},
        {"label": "OKE Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/ContEng/home.htm", "type": "Documentation"},
        {"label": "OKE Cluster Autoscaler Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/ContEng/Tasks/contengusingclusterautoscaler_topic-Working_with_Cluster_Autoscaler_as_Cluster_Add-on.htm", "type": "Documentation"},
    ],
    "opsi_gap": [
        {"label": "OCI Well-Architected Framework", "url": "https://blogs.oracle.com/cloud-infrastructure/oci-wellarchitected-framework", "type": "Blog"},
        {"label": "Operations Insights Documentation", "url": "https://docs.oracle.com/en-us/iaas/operations-insights/home.htm", "type": "Documentation"},
    ],
    "service_connector_gap": [
        {"label": "Service Connector Hub Documentation", "url": "https://docs.oracle.com/en-us/iaas/Content/service-connector-hub/home.htm", "type": "Documentation"},
        {"label": "OpenTelemetry Instrumentation with OCI APM", "url": "https://blogs.oracle.com/cloud-infrastructure/opentelemetry-instrumentation-oci-apm", "type": "Blog"},
    ],
    "vss_gap": [
        {"label": "OCI Container Image Scanning, Signing, and Verification", "url": "https://blogs.oracle.com/cloud-infrastructure/oracle-cloud-infrastructure-container-image-scanning-signing-and-verification", "type": "Blog"},
        {"label": "Vulnerability Scanning Documentation", "url": "https://docs.oracle.com/en-us/iaas/scanning/home.htm", "type": "Documentation"},
    ],
}

CHECK_BUSINESS_VALUE: dict[str, str] = {
    "budgets": "Budgets help customers put clear guardrails around cloud growth so spending stays aligned to business priorities. They also create a shared operating baseline for planning, forecasting, and reducing avoidable surprises as adoption expands on OCI.",
    "cost_anomalies": "Cost anomaly monitoring gives customers an earlier signal when spend patterns drift unexpectedly. That helps teams respond faster, protect budgets more proactively, and keep financial conversations grounded in current OCI usage.",
    "logging": "A logging footprint gives customers the operational history needed to troubleshoot issues faster and understand what changed when services behave unexpectedly. It is a foundational building block for reliable day-to-day operations as more workloads are built on OCI.",
    "monitoring_alarms": "Monitoring alarms help customers move from passive visibility to active operational response. That improves service reliability, shortens time to detect issues, and gives teams more confidence supporting important workloads on OCI.",
    "apm": "Application Performance Monitoring helps customers understand application behavior from the perspective of performance and user impact, not just infrastructure state. That can improve digital experience, reduce troubleshooting time, and support more confident scaling decisions on OCI.",
    "opsi": "Operations Insights helps customers see long-term trends across database and platform services so capacity planning is less reactive. That creates stronger operational clarity and helps teams make smarter growth decisions before performance becomes a business issue.",
    "service_connectors": "Service Connector Hub helps customers automate how telemetry moves across OCI services without relying on one-off integrations. That can simplify operations, improve consistency, and make observability patterns easier to scale as the platform grows.",
    "cloud_guard": "Cloud Guard gives customers a broader view of security posture across compartments and regions from a central OCI-native perspective. That helps reduce blind spots, improve governance, and strengthen confidence in how the tenancy is being operated.",
    "vulnerability_scanning": "Vulnerability scanning helps customers identify higher-priority exposure areas earlier so remediation can be more focused and proactive. Over time, that supports stronger security hygiene and a more repeatable risk-reduction model on OCI.",
    "bastion": "Bastion helps customers support administrative access in a more controlled and auditable way. That can reduce exposure from less-governed access patterns while still enabling the operational work required to run important environments on OCI.",
    "oke": "Oracle Kubernetes Engine helps customers run containerized applications on OCI with a more managed control plane and a clearer platform foundation. That can speed delivery for modern applications while improving consistency, scalability, and operational confidence.",
    "oke_elasticity": "OKE elasticity capabilities help customers align cluster capacity more closely to real application demand as usage changes over time. That can improve efficiency, support better performance, and reduce unnecessary infrastructure overhead.",
    "autoscaling": "Autoscaling helps customers match infrastructure supply more closely to real workload demand instead of relying on constant manual resizing. That can improve responsiveness, reduce operational effort, and support better cost alignment on OCI.",
    "database_platform": "Managed database platform services help customers reduce the operational burden of running critical data services at scale. That can improve resilience, simplify lifecycle work, and free teams to focus more on application and business outcomes.",
    "managed_postgresql": "Managed PostgreSQL can help customers simplify database operations while still supporting familiar PostgreSQL-based application patterns. That can reduce routine maintenance effort and create a cleaner path for growth, performance, and lifecycle management on OCI.",
}


@dataclass
class OpportunitiesArgs:
    auth: str
    config_file: str
    profile: str
    output: Path
    timeout_seconds: float
    max_region_workers: int


def parse_args() -> OpportunitiesArgs:
    parser = argparse.ArgumentParser(description="Build OCI opportunities analysis from OCI APIs.")
    parser.add_argument("--auth", choices=["config", "instance_principal"], default="config")
    parser.add_argument("--config-file", default="~/.oci/config")
    parser.add_argument("--profile", default="DEFAULT")
    parser.add_argument("--output", default=DEFAULT_OUTPUT)
    parser.add_argument(
        "--timeout-seconds",
        type=float,
        default=20.0,
        help="Maximum seconds to wait per OCI list call before continuing.",
    )
    parser.add_argument(
        "--max-region-workers",
        type=int,
        default=3,
        help="Maximum number of regions to process in parallel.",
    )
    ns = parser.parse_args()
    return OpportunitiesArgs(
        auth=ns.auth,
        config_file=ns.config_file,
        profile=ns.profile,
        output=Path(ns.output).expanduser(),
        timeout_seconds=max(5.0, ns.timeout_seconds),
        max_region_workers=max(1, ns.max_region_workers),
    )


def build_identity_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.identity.IdentityClient(**kwargs)


def build_resource_search_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.resource_search.ResourceSearchClient(**kwargs)


def build_budget_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.budget.BudgetClient(**kwargs)


def build_costad_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.budget.CostAdClient(**kwargs)


def build_cloud_guard_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.cloud_guard.CloudGuardClient(**kwargs)


def build_monitoring_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.monitoring.MonitoringClient(**kwargs)


def build_logging_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.logging.LoggingManagementClient(**kwargs)


def build_container_engine_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.container_engine.ContainerEngineClient(**kwargs)


def build_bastion_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.bastion.BastionClient(**kwargs)


def build_vss_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.vulnerability_scanning.VulnerabilityScanningClient(**kwargs)


def build_service_connector_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.sch.ServiceConnectorClient(**kwargs)


def build_autoscaling_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.autoscaling.AutoScalingClient(**kwargs)


def build_database_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.database.DatabaseClient(**kwargs)


def build_apm_domain_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.apm_control_plane.ApmDomainClient(**kwargs)


def build_opsi_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.opsi.OperationsInsightsClient(**kwargs)


def build_adm_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.adm.ApplicationDependencyManagementClient(**kwargs)


def build_load_balancer_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.load_balancer.LoadBalancerClient(**kwargs)


def build_network_load_balancer_client(config: dict[str, Any], signer: Any):
    kwargs = {"config": config}
    if signer is not None:
        kwargs["signer"] = signer
    return oci.network_load_balancer.NetworkLoadBalancerClient(**kwargs)


def score_label(score: int) -> str:
    if score >= 80:
        return "Strong"
    if score >= 60:
        return "Established"
    if score >= 40:
        return "Emerging"
    return "Opportunity"


def clamp_score(value: float) -> int:
    return max(0, min(100, int(round(value))))


def is_active_compute_instance(instance: Any) -> bool:
    lifecycle_state = (getattr(instance, "lifecycle_state", "") or "").upper()
    return lifecycle_state not in {"TERMINATED", "TERMINATING"}


def is_service_unavailable_error(exc: Exception) -> bool:
    return (
        getattr(exc, "status", None) in {403, 404}
        and getattr(exc, "code", "") in {"NotAuthorizedOrNotFound", "NotAuthorized", "NamespaceNotFound"}
    )


def format_service_issue(service_label: str, exc: Exception) -> str:
    if is_service_unavailable_error(exc):
        return f"{service_label} is not enabled or not authorized for this tenancy/profile in the home region; treating it as unavailable."
    if getattr(exc, "status", None) == 400 and getattr(exc, "code", "") == "MissingParameter":
        return f"{service_label} requires additional request parameters; treating it as unavailable for now."
    return f"{service_label} lookup failed in the home region: {exc}"


def safe_get_count(items: list[Any]) -> int:
    return len(items or [])


def safe_collect(
    label: str,
    fn: Callable[[], list[Any]],
    timeout_seconds: float,
) -> tuple[list[Any], str | None]:
    result: dict[str, Any] = {"items": [], "error": None}

    def worker() -> None:
        try:
            result["items"] = fn()
        except Exception as exc:  # pragma: no cover - network/runtime specific
            result["error"] = exc

    thread = threading.Thread(target=worker, daemon=True)
    started = time.time()
    thread.start()
    thread.join(timeout_seconds)
    if thread.is_alive():
        return [], f"{label} exceeded {timeout_seconds:g}s; continuing"
    if result["error"] is not None:
        elapsed = time.time() - started
        return [], f"{label} failed after {elapsed:.2f}s: {result['error']}"
    elapsed = time.time() - started
    log(f"{label} completed in {elapsed:.2f}s with {len(result['items'])} items")
    return list(result["items"]), None


def list_across_compartments(
    base_config: dict[str, Any],
    signer: Any,
    region: str,
    compartments: dict[str, dict[str, str]],
    timeout_seconds: float,
    client_builder: Callable[[dict[str, Any], Any], Any],
    method_name: str,
    label: str,
    extra_kwargs: dict[str, Any] | None = None,
) -> tuple[list[Any], list[str]]:
    regional_config = dict(base_config)
    regional_config["region"] = region
    client = client_builder(regional_config, signer)
    warnings: list[str] = []
    items: list[Any] = []
    extra_kwargs = extra_kwargs or {}
    for compartment_id, compartment in compartments.items():
        compartment_name = compartment.get("name", compartment_id)
        list_method = getattr(client, method_name, None)
        if list_method is None:
            return items, [f"{label} API is not available in the current OCI SDK for {region}; skipping"]

        def call() -> list[Any]:
            response = list_call_get_all_results(list_method, compartment_id=compartment_id, **extra_kwargs)
            return [] if response is None or getattr(response, "data", None) is None else list(response.data)

        item_list, warning = safe_collect(
            f"{label} in {region} for compartment {compartment_name} ({compartment_id})",
            call,
            timeout_seconds,
        )
        if warning:
            warnings.append(warning)
            continue
        items.extend(item_list)
    return items, warnings


def list_cluster_addons(
    base_config: dict[str, Any],
    signer: Any,
    region: str,
    clusters: list[Any],
    timeout_seconds: float,
) -> tuple[list[Any], list[str]]:
    regional_config = dict(base_config)
    regional_config["region"] = region
    client = build_container_engine_client(regional_config, signer)
    warnings: list[str] = []
    addons: list[Any] = []
    for cluster in clusters:
        cluster_id = getattr(cluster, "id", None)
        cluster_name = getattr(cluster, "name", None) or cluster_id or "cluster"
        if not cluster_id:
            continue

        def call() -> list[Any]:
            response = list_call_get_all_results(client.list_addons, cluster_id=cluster_id)
            return [] if response is None or getattr(response, "data", None) is None else list(response.data)

        item_list, warning = safe_collect(
            f"OKE addon listing in {region} for cluster {cluster_name}",
            call,
            timeout_seconds,
        )
        if warning:
            warnings.append(warning)
            continue
        addons.extend(item_list)
    return addons, warnings


def collect_search_inventory(base_config: dict[str, Any], signer: Any, timeout_seconds: float) -> tuple[dict[str, Any], list[str]]:
    client = build_resource_search_client(base_config, signer)
    warnings: list[str] = []

    def call() -> list[Any]:
        details = oci.resource_search.models.StructuredSearchDetails(
            query="query all resources",
            type="Structured",
        )
        response = list_call_get_all_results(client.search_resources, search_details=details)
        return [] if response is None or getattr(response, "data", None) is None else list(response.data)

    items, warning = safe_collect(
        f"OCI Search tenancy inventory via {client.base_client.endpoint}",
        call,
        timeout_seconds * 2,
    )
    if warning:
        warnings.append(warning)

    resource_types = Counter()
    instance_name_signals: list[str] = []
    active_region_counts = Counter()
    for item in items:
        resource_type = (getattr(item, "resource_type", "") or "").strip()
        if resource_type:
            resource_types[resource_type] += 1
        region = getattr(item, "region", "") or ""
        if region:
            active_region_counts[region] += 1
        display_name = (getattr(item, "display_name", "") or "").lower()
        identifier = (getattr(item, "identifier", "") or "").lower()
        if any(keyword in display_name or keyword in identifier for keyword in POSTGRES_KEYWORDS):
            instance_name_signals.append(getattr(item, "display_name", "") or getattr(item, "identifier", ""))

    return {
        "resourceCount": len(items),
        "resourceTypes": dict(resource_types),
        "postgresWorkloadSignals": sorted({value for value in instance_name_signals if value})[:50],
        "activeRegionCounts": dict(active_region_counts),
    }, warnings


def count_matching_resource_types(resource_types: dict[str, int], keywords: tuple[str, ...]) -> int:
    total = 0
    for resource_type, count in resource_types.items():
        lowered = resource_type.lower()
        if any(keyword in lowered for keyword in keywords):
            total += count
    return total


def process_region(
    args: OpportunitiesArgs,
    config: dict[str, Any],
    signer: Any,
    region: str,
    compartments: dict[str, dict[str, str]],
) -> dict[str, Any]:
    emit_event("region_started", region=region, phase="Starting", startedAt=iso_utc(utc_now()))
    log(f"Scanning opportunities in region {region}")

    profile: dict[str, Any] = {
        "name": region,
        "instanceCount": 0,
        "alarmCount": 0,
        "logGroupCount": 0,
        "clusterCount": 0,
        "nodePoolCount": 0,
        "virtualNodePoolCount": 0,
        "clusterAddonCount": 0,
        "autoscalerAddonCount": 0,
        "karpenterAddonCount": 0,
        "bastionCount": 0,
        "hostScanTargetCount": 0,
        "containerScanTargetCount": 0,
        "serviceConnectorCount": 0,
        "autoScalingConfigCount": 0,
        "dbSystemCount": 0,
        "cloudVmClusterCount": 0,
        "apmDomainCount": 0,
        "loadBalancerCount": 0,
        "networkLoadBalancerCount": 0,
        "warnings": [],
    }

    emit_event("region_phase", region=region, phase="Collecting Compute", status="running")
    compute_instances, warnings = list_instances_for_region(
        config, signer, region, compartments, args.timeout_seconds
    )
    profile["instanceCount"] = sum(1 for instance in compute_instances if is_active_compute_instance(instance))
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    emit_event("region_phase", region=region, phase="Collecting Observability", status="running")
    alarms, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_monitoring_client, "list_alarms", "Monitoring alarm listing"
    )
    profile["alarmCount"] = safe_get_count(alarms)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    log_groups, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_logging_client, "list_log_groups", "Logging log group listing"
    )
    profile["logGroupCount"] = safe_get_count(log_groups)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    service_connectors, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_service_connector_client, "list_service_connectors", "Service Connector Hub listing"
    )
    profile["serviceConnectorCount"] = safe_get_count(service_connectors)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    apm_domains, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_apm_domain_client, "list_apm_domains", "APM domain listing"
    )
    profile["apmDomainCount"] = safe_get_count(apm_domains)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    emit_event("region_phase", region=region, phase="Collecting Security", status="running")
    bastions, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_bastion_client, "list_bastions", "Bastion listing"
    )
    profile["bastionCount"] = safe_get_count(bastions)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    host_scan_targets, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_vss_client, "list_host_scan_targets", "Vulnerability scanning host target listing"
    )
    profile["hostScanTargetCount"] = safe_get_count(host_scan_targets)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    container_scan_targets, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_vss_client, "list_container_scan_targets", "Vulnerability scanning container target listing"
    )
    profile["containerScanTargetCount"] = safe_get_count(container_scan_targets)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    emit_event("region_phase", region=region, phase="Collecting Containers", status="running")
    clusters, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_container_engine_client, "list_clusters", "OKE cluster listing"
    )
    profile["clusterCount"] = safe_get_count(clusters)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    node_pools, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_container_engine_client, "list_node_pools", "OKE node pool listing"
    )
    profile["nodePoolCount"] = safe_get_count(node_pools)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    virtual_node_pools, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_container_engine_client, "list_virtual_node_pools", "OKE virtual node pool listing"
    )
    profile["virtualNodePoolCount"] = safe_get_count(virtual_node_pools)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    addons, warnings = list_cluster_addons(config, signer, region, clusters, args.timeout_seconds)
    profile["clusterAddonCount"] = safe_get_count(addons)
    profile["autoscalerAddonCount"] = sum(
        1 for addon in addons
        if "autoscaler" in ((getattr(addon, "addon_name", "") or getattr(addon, "name", "") or "").lower())
    )
    profile["karpenterAddonCount"] = sum(
        1 for addon in addons
        if "karpenter" in ((getattr(addon, "addon_name", "") or getattr(addon, "name", "") or "").lower())
    )
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    auto_scaling_configs, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_autoscaling_client, "list_auto_scaling_configurations", "Autoscaling configuration listing"
    )
    profile["autoScalingConfigCount"] = safe_get_count(auto_scaling_configs)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    emit_event("region_phase", region=region, phase="Collecting Data Platform", status="running")
    db_systems, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_database_client, "list_db_systems", "Database system listing"
    )
    profile["dbSystemCount"] = safe_get_count(db_systems)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    cloud_vm_clusters, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_database_client, "list_cloud_vm_clusters", "Cloud VM cluster listing"
    )
    profile["cloudVmClusterCount"] = safe_get_count(cloud_vm_clusters)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    emit_event("region_phase", region=region, phase="Collecting Traffic Management", status="running")
    load_balancers, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_load_balancer_client, "list_load_balancers", "Load balancer listing"
    )
    profile["loadBalancerCount"] = safe_get_count(load_balancers)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    network_load_balancers, warnings = list_across_compartments(
        config, signer, region, compartments, args.timeout_seconds, build_network_load_balancer_client, "list_network_load_balancers", "Network load balancer listing"
    )
    profile["networkLoadBalancerCount"] = safe_get_count(network_load_balancers)
    profile["warnings"].extend(warnings)
    for warning in warnings:
        log(warning)

    profile["observabilityFootprintCount"] = (
        profile["alarmCount"] + profile["logGroupCount"] + profile["serviceConnectorCount"] + profile["apmDomainCount"]
    )
    profile["securityFootprintCount"] = (
        profile["bastionCount"] + profile["hostScanTargetCount"] + profile["containerScanTargetCount"]
    )
    profile["warningCount"] = len(profile["warnings"])
    emit_event(
        "region_completed",
        region=region,
        finishedAt=iso_utc(utc_now()),
        instanceCount=profile["instanceCount"] + profile["observabilityFootprintCount"] + profile["securityFootprintCount"] + profile["clusterCount"],
    )
    return profile


def build_check(
    check_id: str,
    title: str,
    category: str,
    status: str,
    confidence: str,
    count: int,
    summary: str,
    evidence: list[str],
    scope_label: str,
    regional_counts: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    return {
        "id": check_id,
        "title": title,
        "category": category,
        "status": status,
        "confidence": confidence,
        "count": count,
        "summary": summary,
        "businessValue": CHECK_BUSINESS_VALUE.get(check_id, ""),
        "evidence": evidence,
        "scopeLabel": scope_label,
        "regionalCounts": regional_counts or [],
    }


def compute_category_scores(checks: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for check in checks:
        grouped[check["category"]].append(check)

    scores: list[dict[str, Any]] = []
    for category, items in sorted(grouped.items()):
        applicable = [item for item in items if item["status"] != "not_applicable"]
        adopted = [item for item in applicable if item["status"] == "adopted"]
        score = 0 if not applicable else clamp_score((len(adopted) / len(applicable)) * 100)
        scores.append(
            {
                "id": category.lower().replace(" ", "_"),
                "title": category,
                "score": score,
                "label": score_label(score),
                "summary": f"{len(adopted)} of {len(applicable)} tracked capability checks in {category} currently show an observed OCI footprint.",
            }
        )
    return scores


def build_opportunity(
    opportunity_id: str,
    title: str,
    category: str,
    confidence: str,
    priority: str,
    impact: str,
    summary: str,
    recommendation: str,
    evidence: list[str],
    impacted_count: int,
    regions: list[str],
) -> dict[str, Any]:
    return {
        "id": opportunity_id,
        "title": title,
        "category": category,
        "confidence": confidence,
        "priority": priority,
        "impact": impact,
        "summary": summary,
        "recommendation": recommendation,
        "evidence": evidence,
        "impactedCount": impacted_count,
        "regions": regions,
    }


def dedupe_resources(resources: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str]] = set()
    deduped: list[dict[str, str]] = []
    for item in resources:
        label = (item.get("label") or "").strip()
        url = (item.get("url") or "").strip()
        if not label or not url:
            continue
        key = (label, url)
        if key in seen:
            continue
        seen.add(key)
        deduped.append({
            "label": label,
            "url": url,
            "type": (item.get("type") or "Resource").strip(),
        })
    return deduped


def build_opportunity_resources(opportunity: dict[str, Any]) -> list[dict[str, str]]:
    category = opportunity.get("category", "")
    opportunity_id = opportunity.get("id", "")
    resources = CATEGORY_RESOURCE_LINKS.get(category, []) + OPPORTUNITY_RESOURCE_LINKS.get(opportunity_id, [])
    return dedupe_resources(resources)


def build_email_template(tenancy_name: str, opportunity: dict[str, Any]) -> dict[str, str]:
    category = opportunity.get("category", "OCI")
    title = opportunity.get("title", "OCI opportunity")
    summary = opportunity.get("summary", "")
    recommendation = opportunity.get("recommendation", "")
    impact = opportunity.get("impact", "")
    regions = [region for region in opportunity.get("regions", []) if region]
    region_phrase = ", ".join(regions[:3]) if regions else "the tenancy"
    if len(regions) > 3:
        region_phrase = f"{', '.join(regions[:3])}, and other active regions"

    subject = f"OCI idea for {tenancy_name}: {title}"
    body = (
        f"Hi <Customer Name>,\n\n"
        f"As part of the latest OCI Tenancy Explorer review for {tenancy_name}, one topic that may be worth a short discussion is {title.lower()}.\n\n"
        f"What we observed:\n"
        f"- {summary}\n"
        f"- This was most visible across {region_phrase}.\n\n"
        f"Why it may matter:\n"
        f"- {impact}\n\n"
        f"A practical next step could be:\n"
        f"- {recommendation}\n\n"
        f"If helpful, we can walk through what this could look like in your current OCI footprint and decide whether it is relevant to your team's priorities.\n\n"
        f"Best regards,\n"
        f"<Your Name>\n"
        f"<Title / Team>"
    )
    return {
        "subject": subject,
        "body": body,
    }


def enrich_opportunity(tenancy_name: str, opportunity: dict[str, Any]) -> dict[str, Any]:
    enriched = dict(opportunity)
    enriched["resources"] = build_opportunity_resources(opportunity)
    return enriched


def ensure_category_recommendations(
    tenancy_name: str,
    scores: list[dict[str, Any]],
    checks: list[dict[str, Any]],
    opportunities: list[dict[str, Any]],
    regions: list[str],
    active_regions: list[str],
) -> list[dict[str, Any]]:
    covered_categories = {item["category"] for item in opportunities}
    category_checks: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for check in checks:
        category_checks[check["category"]].append(check)

    for score in scores:
        category = score["title"]
        if category in covered_categories:
            continue

        items = category_checks.get(category, [])
        applicable = [item for item in items if item["status"] != "not_applicable"]
        adopted = [item for item in applicable if item["status"] == "adopted"]
        gaps = [item for item in applicable if item["status"] == "gap"]
        score_value = int(score.get("score", 0) or 0)
        score_label_value = score.get("label", "Opportunity")
        target_regions = active_regions or regions

        if gaps:
            title = f"Build a focused {category.lower()} roadmap"
            priority = "Foundational" if score_value < 40 else "Monitor"
            confidence = "Medium"
            recommendation = (
                f"Use the observed {category.lower()} gaps as a starting point for a focused roadmap conversation tied to the customer's current OCI footprint. "
                f"From there, narrow the discussion to the next one or two capabilities that would create the clearest operational or business value first. "
                f"This keeps the path practical, avoids over-designing too early, and lets the customer build confidence with visible progress over time."
            )
            evidence = [
                f"{len(gaps)} gap-oriented capability check(s) are currently visible in {category}.",
                f"{len(adopted)} adopted capability check(s) are already present in {category}.",
            ]
            if gaps:
                evidence.append(
                    f"Example gap signals: {', '.join(item['title'] for item in gaps[:3])}."
                )
            summary = score["summary"]
            impact = (
                f"Helps the customer prioritize the parts of {category.lower()} that could most improve resilience, efficiency, governance, or day-to-day operational simplicity. "
                f"Instead of treating every capability the same, it highlights where OCI adoption may have the strongest practical payoff for the footprint that already exists today. "
                f"That makes it easier to focus time and investment on the areas most likely to strengthen outcomes for the platform and the teams that depend on it."
            )
            impacted_count = max(1, len(gaps))
        else:
            title = f"Keep {category.lower()} momentum moving"
            priority = "Monitor"
            confidence = "High" if applicable else "Medium"
            recommendation = (
                f"Use the current {category.lower()} posture as a conversation starter and validate whether it already meets the needs of the business and platform teams. "
                f"If the current approach feels strong, this can confirm where the tenancy is already well covered and where no immediate change is required. "
                f"If priorities are evolving, it also provides a clean baseline for deciding whether any new OCI capabilities should be explored over time."
            )
            evidence = [
                f"{len(adopted)} adopted capability check(s) are currently visible in {category}.",
                f"{len(applicable)} applicable capability check(s) were evaluated for {category}.",
            ]
            summary = (
                f"{category} currently reads as {score_label_value.lower()} based on the capabilities this scan could observe."
                if applicable
                else f"The current scan did not observe a strong {category.lower()} footprint, so this can be treated as a discovery topic rather than a gap."
            )
            impact = (
                f"Helps the customer confirm whether the current {category.lower()} posture already supports business priorities or whether there is room to unlock more value on OCI over time. "
                f"This is especially useful when the tenancy already shows adoption and the next question is not \"what is missing,\" but \"what could make the existing platform even stronger.\" "
                f"It gives the customer a clearer view of where OCI is already delivering value and where future improvements might create additional business benefit."
            )
            impacted_count = max(1, len(applicable))

        opportunities.append(build_opportunity(
            f"{score['id']}_roadmap",
            title,
            category,
            confidence,
            priority,
            impact,
            summary,
            recommendation,
            evidence,
            impacted_count,
            target_regions,
        ))
        covered_categories.add(category)

    return [enrich_opportunity(tenancy_name, item) for item in opportunities]


def build_analysis(
    tenancy_name: str,
    regions: list[str],
    compartments: dict[str, dict[str, str]],
    inventory: dict[str, Any],
    home_region_counts: dict[str, int],
    home_region_states: dict[str, str],
    regional_profiles: list[dict[str, Any]],
) -> dict[str, Any]:
    resource_types = inventory.get("resourceTypes", {})
    search_compute_resource_count = count_matching_resource_types(resource_types, ("instance",))
    managed_postgresql_count = count_matching_resource_types(resource_types, ("postgres",))
    postgres_signal_count = len(inventory.get("postgresWorkloadSignals", []))
    total_resource_count = int(inventory.get("resourceCount", 0) or 0)

    aggregated_region_counts = Counter()
    for profile in regional_profiles:
        for key, value in profile.items():
            if key.endswith("Count") and isinstance(value, int):
                aggregated_region_counts[key] += value
    compute_instance_count = int(aggregated_region_counts.get("instanceCount", 0) or 0)

    def region_breakdown(metric_key: str) -> list[dict[str, Any]]:
        return [
            {"region": profile["name"], "count": int(profile.get(metric_key, 0) or 0)}
            for profile in regional_profiles
        ]

    checks = [
        build_check(
            "budgets",
            "Budgets Configured",
            "FinOps",
            "adopted" if home_region_counts["budgetCount"] > 0 else "gap",
            "High",
            home_region_counts["budgetCount"],
            f"{home_region_counts['budgetCount']} OCI budget object(s) detected in the tenancy home region.",
            [f"Direct regional compute scan detected {compute_instance_count} active compute instance(s) across subscribed regions."],
            "Tenancy Home Region",
        ),
        build_check(
            "cost_anomalies",
            "Cost Anomaly Monitoring",
            "FinOps",
            "not_applicable" if home_region_states.get("costAnomalyMonitorCount") == "unavailable" else ("adopted" if home_region_counts["costAnomalyMonitorCount"] > 0 else "gap"),
            "High",
            home_region_counts["costAnomalyMonitorCount"],
            f"{home_region_counts['costAnomalyMonitorCount']} OCI cost anomaly monitor(s) detected.",
            [f"Budget count: {home_region_counts['budgetCount']}."],
            "Tenancy Home Region",
        ),
        build_check(
            "logging",
            "Logging Footprint",
            "Observability",
            "adopted" if aggregated_region_counts["logGroupCount"] > 0 else "gap",
            "High",
            aggregated_region_counts["logGroupCount"],
            f"{aggregated_region_counts['logGroupCount']} OCI log group(s) detected across subscribed regions.",
            [f"Regions with log groups: {sum(1 for profile in regional_profiles if profile['logGroupCount'] > 0)}."],
            "Regional",
            region_breakdown("logGroupCount"),
        ),
        build_check(
            "monitoring_alarms",
            "Monitoring Alarms",
            "Observability",
            "adopted" if aggregated_region_counts["alarmCount"] > 0 else "gap",
            "High",
            aggregated_region_counts["alarmCount"],
            f"{aggregated_region_counts['alarmCount']} OCI monitoring alarm(s) detected across subscribed regions.",
            [f"Regions with alarms: {sum(1 for profile in regional_profiles if profile['alarmCount'] > 0)}."],
            "Regional",
            region_breakdown("alarmCount"),
        ),
        build_check(
            "apm",
            "Application Performance Monitoring",
            "Observability",
            "adopted" if aggregated_region_counts["apmDomainCount"] > 0 else "gap",
            "High",
            aggregated_region_counts["apmDomainCount"],
            f"{aggregated_region_counts['apmDomainCount']} APM domain(s) detected.",
            [f"Observability footprint count: {aggregated_region_counts['observabilityFootprintCount']}."],
            "Regional",
            region_breakdown("apmDomainCount"),
        ),
        build_check(
            "opsi",
            "Operations Insights",
            "Observability",
            "not_applicable" if home_region_states.get("opsiWarehouseCount") == "unavailable" else ("adopted" if home_region_counts["opsiWarehouseCount"] > 0 else "gap"),
            "High",
            home_region_counts["opsiWarehouseCount"],
            f"{home_region_counts['opsiWarehouseCount']} Operations Insights warehouse object(s) detected.",
            [f"Database systems detected: {aggregated_region_counts['dbSystemCount']}."],
            "Tenancy Home Region",
        ),
        build_check(
            "service_connectors",
            "Service Connector Hub",
            "Observability",
            "adopted" if aggregated_region_counts["serviceConnectorCount"] > 0 else "gap",
            "High",
            aggregated_region_counts["serviceConnectorCount"],
            f"{aggregated_region_counts['serviceConnectorCount']} Service Connector Hub connector(s) detected.",
            [f"Log groups detected: {aggregated_region_counts['logGroupCount']}."],
            "Regional",
            region_breakdown("serviceConnectorCount"),
        ),
        build_check(
            "cloud_guard",
            "Cloud Guard",
            "Security",
            "adopted" if home_region_counts["cloudGuardTargetCount"] > 0 else "gap",
            "High",
            home_region_counts["cloudGuardTargetCount"],
            f"{home_region_counts['cloudGuardTargetCount']} Cloud Guard target(s) detected.",
            [f"Subscribed regions: {len(regions)}. Compartments: {len(compartments)}."],
            "Tenancy Home Region",
        ),
        build_check(
            "vulnerability_scanning",
            "Vulnerability Scanning",
            "Security",
            "adopted" if (aggregated_region_counts["hostScanTargetCount"] + aggregated_region_counts["containerScanTargetCount"]) > 0 else "gap",
            "High",
            aggregated_region_counts["hostScanTargetCount"] + aggregated_region_counts["containerScanTargetCount"],
            f"{aggregated_region_counts['hostScanTargetCount']} host scan target(s) and {aggregated_region_counts['containerScanTargetCount']} container scan target(s) detected.",
            [f"Direct regional compute scan detected {compute_instance_count} active compute instance(s) across subscribed regions."],
            "Regional",
            [
                {
                    "region": profile["name"],
                    "count": int(profile.get("hostScanTargetCount", 0) or 0) + int(profile.get("containerScanTargetCount", 0) or 0),
                }
                for profile in regional_profiles
            ],
        ),
        build_check(
            "bastion",
            "Bastion Service",
            "Security",
            "adopted" if aggregated_region_counts["bastionCount"] > 0 else "gap",
            "High",
            aggregated_region_counts["bastionCount"],
            f"{aggregated_region_counts['bastionCount']} bastion resource(s) detected.",
            [f"OKE clusters detected: {aggregated_region_counts['clusterCount']}. Active compute instances detected: {compute_instance_count}."],
            "Regional",
            region_breakdown("bastionCount"),
        ),
        build_check(
            "oke",
            "Oracle Kubernetes Engine",
            "Containers",
            "adopted" if aggregated_region_counts["clusterCount"] > 0 else "not_applicable",
            "High",
            aggregated_region_counts["clusterCount"],
            f"{aggregated_region_counts['clusterCount']} OKE cluster(s) detected.",
            [f"Node pools detected: {aggregated_region_counts['nodePoolCount']}. Virtual node pools: {aggregated_region_counts['virtualNodePoolCount']}."],
            "Regional",
            region_breakdown("clusterCount"),
        ),
        build_check(
            "oke_elasticity",
            "OKE Elasticity Add-Ons",
            "Containers",
            "adopted" if (aggregated_region_counts["autoscalerAddonCount"] + aggregated_region_counts["karpenterAddonCount"]) > 0 else ("gap" if aggregated_region_counts["clusterCount"] > 0 else "not_applicable"),
            "Medium",
            aggregated_region_counts["autoscalerAddonCount"] + aggregated_region_counts["karpenterAddonCount"],
            f"{aggregated_region_counts['autoscalerAddonCount']} autoscaler-related OKE add-on(s) and {aggregated_region_counts['karpenterAddonCount']} Karpenter-related add-on(s) detected.",
            [f"OKE clusters detected: {aggregated_region_counts['clusterCount']}. Autoscaling configs detected: {aggregated_region_counts['autoScalingConfigCount']}."],
            "Regional",
            [
                {
                    "region": profile["name"],
                    "count": int(profile.get("autoscalerAddonCount", 0) or 0) + int(profile.get("karpenterAddonCount", 0) or 0),
                }
                for profile in regional_profiles
            ],
        ),
        build_check(
            "autoscaling",
            "Autoscaling Configurations",
            "Automation",
            "adopted" if aggregated_region_counts["autoScalingConfigCount"] > 0 else "gap",
            "High",
            aggregated_region_counts["autoScalingConfigCount"],
            f"{aggregated_region_counts['autoScalingConfigCount']} autoscaling configuration(s) detected.",
            [f"Load balancers detected: {aggregated_region_counts['loadBalancerCount'] + aggregated_region_counts['networkLoadBalancerCount']}."],
            "Regional",
            region_breakdown("autoScalingConfigCount"),
        ),
        build_check(
            "database_platform",
            "Managed Database Platform",
            "Data Platform",
            "adopted" if (aggregated_region_counts["dbSystemCount"] + aggregated_region_counts["cloudVmClusterCount"]) > 0 else "gap",
            "High",
            aggregated_region_counts["dbSystemCount"] + aggregated_region_counts["cloudVmClusterCount"],
            f"{aggregated_region_counts['dbSystemCount']} DB system(s) and {aggregated_region_counts['cloudVmClusterCount']} Cloud VM cluster(s) detected.",
            [f"PostgreSQL workload name signals detected: {postgres_signal_count}."],
            "Regional",
            [
                {
                    "region": profile["name"],
                    "count": int(profile.get("dbSystemCount", 0) or 0) + int(profile.get("cloudVmClusterCount", 0) or 0),
                }
                for profile in regional_profiles
            ],
        ),
        build_check(
            "managed_postgresql",
            "Managed PostgreSQL Footprint",
            "Data Platform",
            "adopted" if managed_postgresql_count > 0 else ("gap" if postgres_signal_count > 0 else "not_applicable"),
            "Low" if postgres_signal_count > 0 and managed_postgresql_count == 0 else "High",
            managed_postgresql_count,
            f"{managed_postgresql_count} managed PostgreSQL-related resource(s) detected from OCI Search inventory.",
            [f"PostgreSQL workload name signals detected from OCI inventory: {postgres_signal_count}."],
            "Tenancy Inventory",
        ),
    ]

    scores = compute_category_scores(checks)

    opportunities: list[dict[str, Any]] = []
    active_regions = [
        profile["name"]
        for profile in regional_profiles
        if profile.get("instanceCount", 0)
        or profile.get("observabilityFootprintCount", 0)
        or profile.get("securityFootprintCount", 0)
        or profile.get("clusterCount", 0)
        or profile.get("dbSystemCount", 0)
    ]

    def regions_matching(key: str) -> list[str]:
        return [profile["name"] for profile in regional_profiles if profile.get(key, 0) > 0]

    if home_region_counts["budgetCount"] == 0:
        opportunities.append(build_opportunity(
            "budgets_gap",
            "Introduce tenancy budgets and thresholds",
            "FinOps",
            "High",
            "Quick Win",
            "Helps the customer set clearer spending guardrails as OCI usage grows and new workloads are added over time. Budgets create an early planning signal so teams can see whether usage is tracking to expectations before it turns into a surprise. They also make it easier to connect cloud consumption back to business priorities, environments, or operating models that matter most.",
            "The tenancy currently shows resource footprint but no OCI budget objects in the home region.",
            "A practical next step could be to define a small number of budgets around major environments, business domains, or shared platforms. That gives the customer a lightweight way to introduce visibility without adding unnecessary process overhead. Over time, those thresholds can become part of a stronger operating rhythm for cloud cost awareness and planning.",
            [f"Active compute instances detected across subscribed regions: {compute_instance_count}.", f"Budget count: {home_region_counts['budgetCount']}."],
            max(1, compute_instance_count),
            regions,
        ))

    if home_region_counts["costAnomalyMonitorCount"] == 0 and home_region_states.get("costAnomalyMonitorCount") != "unavailable":
        opportunities.append(build_opportunity(
            "cost_anomaly_gap",
            "Enable cost anomaly monitoring",
            "FinOps",
            "High",
            "Quick Win",
            "Helps the customer spot unusual spend patterns earlier rather than discovering them after a billing cycle has already moved. That can reduce the chance of unexpected cost spikes, especially as more services, projects, or teams begin to use the tenancy. It also gives operations and finance stakeholders a faster signal when actual usage starts to drift from normal patterns.",
            "No OCI cost anomaly monitor objects were detected in the tenancy home region.",
            "A practical next step could be to enable anomaly monitoring alongside existing budgeting or reporting practices. This creates a simple early-warning layer that complements standard cost reviews rather than replacing them. In day-to-day operations, it helps the customer react sooner when something unusual appears in the spend profile.",
            [f"Cost anomaly monitors detected: {home_region_counts['costAnomalyMonitorCount']}.", f"Budget count: {home_region_counts['budgetCount']}."],
            1,
            regions,
        ))

    if aggregated_region_counts["logGroupCount"] > 0 and (
        aggregated_region_counts["alarmCount"] + aggregated_region_counts["apmDomainCount"] + home_region_counts["opsiWarehouseCount"]
    ) == 0:
        opportunities.append(build_opportunity(
            "observability_gap",
            "Expand from logging into broader observability",
            "Observability",
            "High",
            "Important",
            "Builds on existing telemetry to improve service visibility, speed up incident response, and give teams more confidence in day-to-day operations. Logging is often the first step, but broader observability helps teams understand health, performance, and change impact much faster when something matters to the business. That becomes more valuable as applications, environments, and operational dependencies grow across the tenancy.",
            "Logging is present, but the current tenancy scan did not find a broader observability footprint through alarms, APM, or Operations Insights.",
            "A practical next step could be to connect existing logging with monitoring alarms and, where appropriate, APM or Operations Insights. This creates a more complete operational baseline so teams can move from simply collecting data to acting on it more effectively. It also helps the customer build a stronger reliability story on OCI without having to start from scratch.",
            [f"Log groups detected: {aggregated_region_counts['logGroupCount']}.", f"Monitoring alarms detected: {aggregated_region_counts['alarmCount']}.", f"APM domains detected: {aggregated_region_counts['apmDomainCount']}.", f"Operations Insights warehouses detected: {home_region_counts['opsiWarehouseCount']}."],
            aggregated_region_counts["logGroupCount"],
            regions_matching("logGroupCount"),
        ))

    if aggregated_region_counts["logGroupCount"] > 0 and aggregated_region_counts["serviceConnectorCount"] == 0:
        opportunities.append(build_opportunity(
            "service_connector_gap",
            "Review service-to-service log routing automation",
            "Observability",
            "High",
            "Quick Win",
            "Can reduce manual telemetry handoffs, make logging flows more consistent across environments, and simplify operations as the platform grows. As more services are added, automated routing becomes increasingly useful for keeping observability patterns organized and repeatable. That can save teams time while also reducing the risk of inconsistent operational practices between regions or environments.",
            "The tenancy shows logging footprint, but no Service Connector Hub connectors were detected.",
            "If logs need to move into analytics, notifications, archival paths, or downstream services, Service Connector Hub may be worth reviewing. It gives the customer a more structured way to extend telemetry workflows without relying on one-off integrations everywhere. That can make the observability architecture easier to scale and easier to operate over time.",
            [f"Log groups detected: {aggregated_region_counts['logGroupCount']}.", f"Service connectors detected: {aggregated_region_counts['serviceConnectorCount']}."],
            aggregated_region_counts["logGroupCount"],
            regions_matching("logGroupCount"),
        ))

    if home_region_counts["cloudGuardTargetCount"] == 0:
        opportunities.append(build_opportunity(
            "cloud_guard_gap",
            "Review Cloud Guard tenancy coverage",
            "Security",
            "High",
            "Strategic",
            "Can give the customer broader security posture visibility and a more consistent way to detect issues across a growing tenancy footprint before they affect critical workloads. This is especially valuable as compartments, regions, and shared services increase over time. A clearer posture view helps teams manage security with more confidence while reducing blind spots across the tenancy.",
            "No Cloud Guard targets were detected in the tenancy home region.",
            "A practical next step could be to review whether Cloud Guard coverage aligns with the customer's current operating model and security goals. If the answer is yes, it can provide a stronger baseline for visibility and governance across the tenancy. If not, the discussion still helps clarify which security signals matter most for the environments already in place.",
            [f"Cloud Guard target count: {home_region_counts['cloudGuardTargetCount']}.", f"Compartments detected: {len(compartments)}."],
            len(compartments),
            regions,
        ))

    if (aggregated_region_counts["hostScanTargetCount"] + aggregated_region_counts["containerScanTargetCount"]) == 0 and (compute_instance_count > 0 or aggregated_region_counts["clusterCount"] > 0):
        opportunities.append(build_opportunity(
            "vss_gap",
            "Expand vulnerability scanning coverage",
            "Security",
            "High",
            "Important",
            "Improves security hygiene, helps the customer identify higher-priority remediation areas earlier, and supports a more proactive security posture on OCI. As compute and container usage grows, having a clearer view of exposure can make remediation work more focused and less reactive. It also helps platform and security teams align around a more repeatable baseline for ongoing risk reduction.",
            "The tenancy shows compute or container footprint, but no vulnerability scanning targets were detected.",
            "A practical next step could be to review whether host or container scanning makes sense for the parts of the footprint that matter most from a risk and compliance perspective. The goal is not to scan everything at once, but to start where visibility would create the clearest operational value. That approach helps the customer strengthen posture in a manageable, incremental way.",
            [f"Host scan targets detected: {aggregated_region_counts['hostScanTargetCount']}.", f"Container scan targets detected: {aggregated_region_counts['containerScanTargetCount']}.", f"OKE clusters detected: {aggregated_region_counts['clusterCount']}."],
            max(compute_instance_count, aggregated_region_counts["clusterCount"]),
            regions,
        ))

    if aggregated_region_counts["bastionCount"] == 0 and (compute_instance_count > 0 or aggregated_region_counts["clusterCount"] > 0):
        opportunities.append(build_opportunity(
            "bastion_gap",
            "Review secure administrative access patterns",
            "Security",
            "High",
            "Quick Win",
            "Can strengthen administrative access controls, reduce exposure from less-governed access patterns, and simplify secure operations for cloud-hosted workloads. Administrative access is often necessary, but it benefits from being structured in a way that is easier to audit and manage consistently. A stronger access pattern can improve both security posture and day-to-day operational confidence.",
            "The tenancy shows compute or OKE footprint, but no OCI Bastion resources were detected.",
            "If administrators still need interactive access into the environment, OCI Bastion may be worth reviewing as a more controlled access path. This can help the customer reduce reliance on less-governed approaches while still supporting real operational needs. It is also a practical way to strengthen access hygiene without changing the overall workload architecture.",
            [f"Bastion resources detected: {aggregated_region_counts['bastionCount']}.", f"Active compute instances detected: {compute_instance_count}.", f"OKE clusters detected: {aggregated_region_counts['clusterCount']}."],
            max(compute_instance_count, aggregated_region_counts["clusterCount"]),
            regions,
        ))

    if aggregated_region_counts["clusterCount"] > 0 and (aggregated_region_counts["autoscalerAddonCount"] + aggregated_region_counts["karpenterAddonCount"]) == 0:
        opportunities.append(build_opportunity(
            "oke_elasticity_gap",
            "Review managed OKE elasticity patterns",
            "Containers",
            "Medium",
            "Strategic",
            "Can improve cluster efficiency, elasticity, and cost alignment as Kubernetes usage grows, especially where demand changes over time. Elasticity becomes more valuable when application demand is uneven, environments expand, or teams want better infrastructure responsiveness without constant manual tuning. That can translate into both operational simplicity and better alignment between capacity and actual usage.",
            "OKE clusters are present, but the current scan did not detect Karpenter-related or autoscaler-related add-ons.",
            "A practical next step could be to evaluate whether managed elasticity patterns such as cluster autoscaler or Karpenter are appropriate for this Kubernetes footprint. The goal is to understand whether the current platform would benefit from more adaptive scaling behavior, not to assume every cluster needs the same pattern. That gives the customer a more thoughtful path to improving performance and efficiency on OCI.",
            [f"OKE clusters detected: {aggregated_region_counts['clusterCount']}.", f"Autoscaler add-ons detected: {aggregated_region_counts['autoscalerAddonCount']}.", f"Karpenter add-ons detected: {aggregated_region_counts['karpenterAddonCount']}."],
            aggregated_region_counts["clusterCount"],
            regions_matching("clusterCount"),
        ))

    if aggregated_region_counts["clusterCount"] > 0 and (aggregated_region_counts["alarmCount"] + aggregated_region_counts["logGroupCount"]) == 0:
        opportunities.append(build_opportunity(
            "oke_observability_gap",
            "Add stronger observability around OKE footprint",
            "Containers",
            "High",
            "Important",
            "Gives the customer a stronger operational baseline for cluster health, incident response, and service reliability as containerized applications scale. Container platforms are powerful, but they are much easier to operate well when health and telemetry signals are easier to interpret. A stronger observability baseline can help teams reduce troubleshooting time and improve confidence in platform stability.",
            "OKE clusters are present, but the current regional scan did not find associated logging or alarm footprint.",
            "A practical next step could be to review whether OKE operations would benefit from a clearer observability baseline through logging and monitoring alarms. That baseline does not need to be overly complex to be valuable; even a focused set of signals can materially improve operations. Over time, it can become part of a more mature reliability model for containerized workloads on OCI.",
            [f"OKE clusters detected: {aggregated_region_counts['clusterCount']}.", f"Log groups detected: {aggregated_region_counts['logGroupCount']}.", f"Monitoring alarms detected: {aggregated_region_counts['alarmCount']}."],
            aggregated_region_counts["clusterCount"],
            regions_matching("clusterCount"),
        ))

    if aggregated_region_counts["autoScalingConfigCount"] == 0 and compute_instance_count > 0:
        opportunities.append(build_opportunity(
            "autoscaling_gap",
            "Review autoscaling opportunities for elastic tiers",
            "Automation",
            "High",
            "Strategic",
            "Can reduce manual scaling effort, improve responsiveness during demand changes, and align infrastructure cost more closely to real workload demand. This is especially useful for customer-facing services or elastic application tiers where usage patterns are not constant. Over time, better scaling behavior can improve both operational efficiency and the customer experience those workloads support.",
            "The tenancy shows compute and traffic-management footprint, but no autoscaling configurations were detected.",
            "A practical next step could be to review which workloads in the current footprint are truly elastic and would benefit from more adaptive scaling. If the answer is only a subset, the customer can start there and measure value before expanding further. That keeps the approach grounded in real workload behavior instead of adopting automation for its own sake.",
            [f"Autoscaling configurations detected: {aggregated_region_counts['autoScalingConfigCount']}.", f"Load balancer footprint: {aggregated_region_counts['loadBalancerCount'] + aggregated_region_counts['networkLoadBalancerCount']}."],
            compute_instance_count,
            regions,
        ))

    if postgres_signal_count > 0 and managed_postgresql_count == 0:
        opportunities.append(build_opportunity(
            "managed_postgresql_gap",
            "Review managed PostgreSQL adoption",
            "Data Platform",
            "Low",
            "Strategic",
            "Can help the customer evaluate whether PostgreSQL workloads could be simplified through a more managed operating model on OCI, reducing operational overhead over time. For many teams, managed database services can free up time that would otherwise go into patching, lifecycle work, backups, and routine administration. That can let teams spend more energy on application delivery and data outcomes rather than platform maintenance.",
            "The scan found PostgreSQL-like workload signals in OCI inventory, but it did not find managed PostgreSQL-related OCI resources.",
            "A practical next step could be to validate whether the PostgreSQL footprint is customer-managed today and whether that model is still the right fit. If managed PostgreSQL aligns better to operational goals, it may create a simpler lifecycle path without changing the core application outcome. Even if no change is needed now, the customer gains a clearer view of future options on OCI.",
            [f"PostgreSQL workload name signals detected: {postgres_signal_count}.", f"Managed PostgreSQL-related OCI resources detected: {managed_postgresql_count}."],
            postgres_signal_count,
            regions,
        ))

    if aggregated_region_counts["dbSystemCount"] > 0 and home_region_counts["opsiWarehouseCount"] == 0 and home_region_states.get("opsiWarehouseCount") != "unavailable":
        opportunities.append(build_opportunity(
            "opsi_gap",
            "Review Operations Insights for database visibility",
            "Data Platform",
            "High",
            "Important",
            "Can improve trend visibility, capacity planning, and operational clarity for managed database services running on OCI as database estates grow more complex. As data platforms expand, it becomes more important to understand performance patterns and capacity signals before they affect business-critical applications. Better visibility can help the customer plan ahead with more confidence and less operational guesswork.",
            "Managed database platform footprint is present, but Operations Insights was not detected.",
            "A practical next step could be to review whether stronger trend and capacity visibility would help the current database estate. If so, Operations Insights may provide a more structured way to understand how those services are behaving over time. That makes it easier to support growth, performance planning, and operational consistency on OCI.",
            [f"DB systems detected: {aggregated_region_counts['dbSystemCount']}.", f"Operations Insights warehouses detected: {home_region_counts['opsiWarehouseCount']}."],
            aggregated_region_counts["dbSystemCount"],
            regions_matching("dbSystemCount"),
        ))

    opportunities = ensure_category_recommendations(
        tenancy_name,
        scores,
        checks,
        opportunities,
        regions,
        active_regions,
    )

    priority_rank = {"Important": 0, "Strategic": 1, "Quick Win": 2, "Foundational": 3, "Monitor": 4}
    confidence_rank = {"High": 0, "Medium": 1, "Low": 2}
    opportunities.sort(key=lambda item: (priority_rank.get(item["priority"], 99), confidence_rank.get(item["confidence"], 99), -item["impactedCount"], item["title"]))

    for profile in regional_profiles:
        footprint_total = profile["observabilityFootprintCount"] + profile["securityFootprintCount"] + profile["clusterCount"] + profile["dbSystemCount"] + profile["autoScalingConfigCount"]
        profile["footprintScore"] = clamp_score((footprint_total / 15) * 100)

    return {
        "scopeName": tenancy_name,
        "summary": {
            "resourceCount": total_resource_count,
            "computeInstanceCount": compute_instance_count,
            "subscribedRegionCount": len(regions),
            "activeRegionCount": len(active_regions),
            "compartmentCount": len(compartments),
            "opportunityCount": len(opportunities),
            "adoptedCheckCount": len([check for check in checks if check["status"] == "adopted"]),
            "gapCheckCount": len([check for check in checks if check["status"] == "gap"]),
        },
        "metrics": {
            **home_region_counts,
            **dict(aggregated_region_counts),
            "managedPostgresqlCount": managed_postgresql_count,
            "postgresWorkloadSignalCount": postgres_signal_count,
            "resourceCount": total_resource_count,
            "computeInstanceCount": compute_instance_count,
            "searchComputeResourceCount": search_compute_resource_count,
        },
        "categoryScores": scores,
        "checks": checks,
        "regionProfiles": regional_profiles,
        "opportunities": opportunities,
        "inventory": {
            "resourceTypes": resource_types,
            "postgresWorkloadSignals": inventory.get("postgresWorkloadSignals", []),
        },
        "guardrails": [
            "Insights in this area are based on observed OCI tenancy signals and current platform footprint, not a directive to consume a specific OCI service.",
            "Confidence levels reflect how directly each insight is supported by the OCI data this opportunities scan was able to query.",
            "Some opportunities are intentionally heuristic, especially when inferring customer-managed technologies such as PostgreSQL from OCI inventory patterns.",
        ],
    }


def main() -> int:
    require_oci_sdk()
    args = parse_args()
    try:
        config, signer = load_signer_and_config(args)
    except Exception as exc:
        raise SystemExit(
            "Unable to initialize OCI authentication for the opportunities scan.\n"
            "If you are running locally, create ~/.oci/config or pass --config-file.\n"
            "If you are running on OCI compute, try --auth instance_principal.\n"
            f"Details: {exc}"
        ) from exc

    log(f"Starting OCI opportunities build with profile '{args.profile if args.auth == 'config' else 'instance_principal'}'")
    tenancy_id = get_tenancy_id(config, signer)
    identity_client = build_identity_client(config, signer)
    tenancy_name = resolve_tenancy_name(identity_client, tenancy_id)
    log(f"Connected to tenancy: {tenancy_name} ({tenancy_id})")
    compartments = list_compartments(identity_client, tenancy_id)
    log(f"Loaded {len(compartments)} accessible compartments")
    regions = list_regions(identity_client, tenancy_id)
    log(f"Scanning subscribed regions for opportunities: {', '.join(regions)}")
    emit_event("regions_discovered", regions=regions)

    inventory, inventory_warnings = collect_search_inventory(config, signer, args.timeout_seconds)
    for warning in inventory_warnings:
        log(warning)

    home_region_counts: dict[str, int] = {
        "budgetCount": 0,
        "costAnomalyMonitorCount": 0,
        "cloudGuardTargetCount": 0,
        "opsiWarehouseCount": 0,
        "admKnowledgeBaseCount": 0,
    }
    home_region_states: dict[str, str] = {
        "budgetCount": "available",
        "costAnomalyMonitorCount": "available",
        "cloudGuardTargetCount": "available",
        "opsiWarehouseCount": "available",
        "admKnowledgeBaseCount": "available",
    }

    log("Collecting tenancy-wide home-region opportunity signals")
    try:
        budget_client = build_budget_client(config, signer)
        response = list_call_get_all_results(budget_client.list_budgets, compartment_id=tenancy_id)
        home_region_counts["budgetCount"] = 0 if response is None or getattr(response, "data", None) is None else len(response.data)
        log(f"Budget listing completed in home region with {home_region_counts['budgetCount']} budgets")
    except Exception as exc:
        home_region_states["budgetCount"] = "unavailable" if is_service_unavailable_error(exc) else "error"
        log(format_service_issue("Budget service", exc))

    try:
        costad_client = build_costad_client(config, signer)
        response = list_call_get_all_results(costad_client.list_cost_anomaly_monitors, compartment_id=tenancy_id)
        home_region_counts["costAnomalyMonitorCount"] = 0 if response is None or getattr(response, "data", None) is None else len(response.data)
        log(f"Cost anomaly monitor listing completed in home region with {home_region_counts['costAnomalyMonitorCount']} monitors")
    except Exception as exc:
        home_region_states["costAnomalyMonitorCount"] = "unavailable" if is_service_unavailable_error(exc) else "error"
        log(format_service_issue("Cost anomaly monitoring", exc))

    try:
        cloud_guard_client = build_cloud_guard_client(config, signer)
        response = list_call_get_all_results(cloud_guard_client.list_targets, compartment_id=tenancy_id)
        home_region_counts["cloudGuardTargetCount"] = 0 if response is None or getattr(response, "data", None) is None else len(response.data)
        log(f"Cloud Guard target listing completed in home region with {home_region_counts['cloudGuardTargetCount']} targets")
    except Exception as exc:
        home_region_states["cloudGuardTargetCount"] = "unavailable" if is_service_unavailable_error(exc) else "error"
        log(format_service_issue("Cloud Guard", exc))

    try:
        opsi_client = build_opsi_client(config, signer)
        response = list_call_get_all_results(opsi_client.list_operations_insights_warehouses)
        home_region_counts["opsiWarehouseCount"] = 0 if response is None or getattr(response, "data", None) is None else len(response.data)
        log(f"Operations Insights warehouse listing completed in home region with {home_region_counts['opsiWarehouseCount']} warehouses")
    except Exception as exc:
        home_region_states["opsiWarehouseCount"] = "unavailable" if is_service_unavailable_error(exc) else "error"
        log(format_service_issue("Operations Insights", exc))

    try:
        adm_client = build_adm_client(config, signer)
        response = list_call_get_all_results(adm_client.list_knowledge_bases, compartment_id=tenancy_id)
        home_region_counts["admKnowledgeBaseCount"] = 0 if response is None or getattr(response, "data", None) is None else len(response.data)
        log(f"ADM knowledge base listing completed in home region with {home_region_counts['admKnowledgeBaseCount']} knowledge bases")
    except Exception as exc:
        home_region_states["admKnowledgeBaseCount"] = "unavailable" if is_service_unavailable_error(exc) else "error"
        log(format_service_issue("Application Dependency Management", exc))

    regional_profiles: list[dict[str, Any]] = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(args.max_region_workers, max(1, len(regions)))) as executor:
        future_to_region = {
            executor.submit(process_region, args, config, signer, region, compartments): region
            for region in regions
        }
        for future in concurrent.futures.as_completed(future_to_region):
            region = future_to_region[future]
            try:
                regional_profiles.append(future.result())
            except Exception as exc:  # pragma: no cover - runtime specific
                log(f"Region {region} failed unexpectedly during opportunities scan: {exc}")
                emit_event("region_failed", region=region, phase="Failed", finishedAt=iso_utc(utc_now()))

    regional_profiles.sort(key=lambda item: item["name"])
    generated_at = utc_now()
    payload = {
        "generatedAt": iso_utc(generated_at),
        "generatedAtEpochMs": int(generated_at.timestamp() * 1000),
        "schemaVersion": 1,
        "source": {
            "type": "oci-sdk",
            "auth": args.auth,
            "profile": args.profile if args.auth == "config" else "instance_principal",
            "regions": regions,
        },
        "analysis": build_analysis(tenancy_name, regions, compartments, inventory, home_region_counts, home_region_states, regional_profiles),
    }
    args.output.write_text(json.dumps(payload, indent=2))
    log(f"Wrote OCI opportunities analysis to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
