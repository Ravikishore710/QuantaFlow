"""Unit tests verifying the column-level data lineage graph and OpenLineage exporter."""

import json
from pathlib import Path

from src.lineage.tracker import LineageGraph


def test_lineage_graph_initialization() -> None:
    """Verify that default Medallion lineage edges are populated."""
    graph = LineageGraph()
    assert len(graph.edges) > 20

    # Verify Bronze -> Silver and Silver -> Gold presence
    target_datasets = {e.target_dataset for e in graph.edges}
    assert "bronze_taxi_trips" in target_datasets
    assert "silver_enriched_trips" in target_datasets
    assert "gold_hourly_trip_metrics" in target_datasets
    assert "gold_zone_daily_metrics" in target_datasets
    assert "gold_weather_impact_metrics" in target_datasets


def test_lineage_upstream_lookup() -> None:
    """Verify querying upstream source columns for enriched columns."""
    graph = LineageGraph()

    # Duration depends on dropoff datetime
    up_duration = graph.get_upstream_columns("silver_enriched_trips", "trip_duration_minutes")
    assert len(up_duration) > 0
    assert any(e.source_column == "tpep_dropoff_datetime" for e in up_duration)

    # Weather impact metrics depends on silver temp_celsius
    up_weather = graph.get_upstream_columns("gold_weather_impact_metrics", "temp_celsius")
    assert len(up_weather) > 0
    assert up_weather[0].source_dataset == "silver_enriched_trips"


def test_lineage_downstream_lookup() -> None:
    """Verify querying downstream impacted columns from a raw/bronze column."""
    graph = LineageGraph()

    down = graph.get_downstream_columns("bronze_taxi_trips", "tpep_dropoff_datetime")
    assert len(down) >= 2
    down_cols = {e.target_column for e in down}
    assert "trip_duration_minutes" in down_cols
    assert "tpep_dropoff_datetime" in down_cols


def test_lineage_custom_edge() -> None:
    """Verify adding custom edges to graph."""
    graph = LineageGraph()
    graph.add_edge(
        source_dataset="custom_src",
        source_column="raw_c",
        target_dataset="custom_tgt",
        target_column="clean_c",
        transformation_expr="upper()",
    )
    res = graph.get_upstream_columns("custom_tgt", "clean_c")
    assert len(res) == 1
    assert res[0].transformation_expr == "upper()"


def test_openlineage_event_export(tmp_path: Path) -> None:
    """Verify exporting compliant OpenLineage event JSON."""
    graph = LineageGraph()
    run_id = "test-run-12345"
    period = "2024-01"

    out_file = graph.export_openlineage_event(run_id=run_id, period=period, output_dir=tmp_path)
    assert out_file.is_file()
    assert out_file.name == "lineage_2024-01.json"

    data = json.loads(out_file.read_text(encoding="utf-8"))
    assert data["eventType"] == "COMPLETE"
    assert data["run"]["runId"] == run_id
    assert "quanta_flow_pipeline_2024-01" in data["job"]["name"]
    assert len(data["inputs"]) >= 3
    assert len(data["outputs"]) >= 4

    # Verify columnLineage facet
    sample_output = data["outputs"][0]
    assert "columnLineage" in sample_output["facets"]
    assert "fields" in sample_output["facets"]["columnLineage"]
