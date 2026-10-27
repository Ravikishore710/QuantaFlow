"""Programmatic column-level data lineage graph and OpenLineage exporter.

Tracks data origin, transformations, and schema flow across Medallion layers.
"""

import json
from dataclasses import asdict, dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any


@dataclass
class ColumnLineageEdge:
    """Represents a transformation edge from an upstream column to a downstream column."""

    source_dataset: str
    source_column: str
    target_dataset: str
    target_column: str
    transformation_expr: str


class LineageGraph:
    """Directed graph capturing column-level lineage across Bronze, Silver, and Gold datasets."""

    def __init__(self) -> None:
        self.edges: list[ColumnLineageEdge] = []
        self._initialize_medallion_lineage()

    def add_edge(
        self,
        source_dataset: str,
        source_column: str,
        target_dataset: str,
        target_column: str,
        transformation_expr: str,
    ) -> None:
        """Register a transformation relationship between two columns."""
        self.edges.append(
            ColumnLineageEdge(
                source_dataset=source_dataset,
                source_column=source_column,
                target_dataset=target_dataset,
                target_column=target_column,
                transformation_expr=transformation_expr,
            )
        )

    def _initialize_medallion_lineage(self) -> None:
        """Populate standard QuantaFlow Medallion lineage edges."""
        # 1. Raw -> Bronze Taxi
        raw_taxi_cols = [
            ("VendorID", "vendor_id"),
            ("tpep_pickup_datetime", "tpep_pickup_datetime"),
            ("tpep_dropoff_datetime", "tpep_dropoff_datetime"),
            ("passenger_count", "passenger_count"),
            ("trip_distance", "trip_distance"),
            ("PULocationID", "pulocation_id"),
            ("DOLocationID", "dolocation_id"),
            ("fare_amount", "fare_amount"),
            ("tip_amount", "tip_amount"),
            ("total_amount", "total_amount"),
        ]
        for src, tgt in raw_taxi_cols:
            self.add_edge("raw_tlc_yellow", src, "bronze_taxi_trips", tgt, "identity_cast")

        self.add_edge(
            "metadata",
            "ingest_timestamp",
            "bronze_taxi_trips",
            "_ingested_at",
            "system_timestamp()",
        )
        self.add_edge(
            "metadata", "file_name", "bronze_taxi_trips", "_source_file", "source_file_path()"
        )
        self.add_edge(
            "metadata", "execution_period", "bronze_taxi_trips", "_period", "period_partition()"
        )

        # 2. Raw -> Bronze Weather
        raw_weather_cols = [
            ("DATE", "timestamp"),
            ("STATION", "station_id"),
            ("temperature", "temperature"),
            ("dew_point", "dew_point"),
            ("wind_speed", "wind_speed"),
            ("precipitation", "precipitation"),
        ]
        for src, tgt in raw_weather_cols:
            expr = "to_datetime(UTC)" if src == "DATE" else "identity_cast"
            self.add_edge("raw_noaa_ghcnh", src, "bronze_weather_observations", tgt, expr)

        # 3. Raw -> Bronze Taxi Zones
        raw_zone_cols = [
            ("LocationID", "location_id"),
            ("Borough", "borough"),
            ("Zone", "zone"),
            ("service_zone", "service_zone"),
        ]
        for src, tgt in raw_zone_cols:
            self.add_edge("raw_taxi_zones", src, "bronze_taxi_zones", tgt, "identity_cast")

        # 4. Bronze -> Silver Enriched Trips
        for col in [
            "vendor_id",
            "tpep_pickup_datetime",
            "tpep_dropoff_datetime",
            "passenger_count",
            "trip_distance",
            "pulocation_id",
            "dolocation_id",
            "fare_amount",
            "tip_amount",
            "total_amount",
        ]:
            self.add_edge(
                "bronze_taxi_trips", col, "silver_enriched_trips", col, "physics_bound_filter"
            )

        self.add_edge(
            "bronze_taxi_trips",
            "tpep_dropoff_datetime",
            "silver_enriched_trips",
            "trip_duration_minutes",
            "(dropoff - pickup).total_seconds() / 60.0",
        )
        self.add_edge(
            "bronze_taxi_zones",
            "borough",
            "silver_enriched_trips",
            "pickup_borough",
            "left_join(bronze_taxi_trips.pulocation_id == bronze_taxi_zones.location_id)",
        )
        self.add_edge(
            "bronze_taxi_zones",
            "zone",
            "silver_enriched_trips",
            "pickup_zone",
            "left_join(bronze_taxi_trips.pulocation_id == bronze_taxi_zones.location_id)",
        )
        self.add_edge(
            "bronze_weather_observations",
            "temperature",
            "silver_enriched_trips",
            "temp_celsius",
            "left_join(pickup_hour == weather.timestamp)",
        )
        self.add_edge(
            "bronze_weather_observations",
            "precipitation",
            "silver_enriched_trips",
            "precipitation_mm",
            "left_join(pickup_hour == weather.timestamp)",
        )

        # 5. Silver -> Gold Marts
        # hourly_trip_metrics
        self.add_edge(
            "silver_enriched_trips",
            "tpep_pickup_datetime",
            "gold_hourly_trip_metrics",
            "pickup_hour",
            "date_trunc('hour')",
        )
        self.add_edge(
            "silver_enriched_trips",
            "trip_distance",
            "gold_hourly_trip_metrics",
            "avg_trip_distance",
            "avg(trip_distance)",
        )
        self.add_edge(
            "silver_enriched_trips",
            "fare_amount",
            "gold_hourly_trip_metrics",
            "total_fare_revenue",
            "sum(fare_amount)",
        )
        self.add_edge(
            "silver_enriched_trips",
            "trip_duration_minutes",
            "gold_hourly_trip_metrics",
            "avg_duration_minutes",
            "avg(trip_duration_minutes)",
        )
        self.add_edge(
            "silver_enriched_trips",
            "vendor_id",
            "gold_hourly_trip_metrics",
            "trip_count",
            "count()",
        )

        # zone_daily_metrics
        self.add_edge(
            "silver_enriched_trips",
            "pickup_borough",
            "gold_zone_daily_metrics",
            "pickup_borough",
            "group_by()",
        )
        self.add_edge(
            "silver_enriched_trips",
            "pickup_zone",
            "gold_zone_daily_metrics",
            "pickup_zone",
            "group_by()",
        )
        self.add_edge(
            "silver_enriched_trips",
            "fare_amount",
            "gold_zone_daily_metrics",
            "total_fare_revenue",
            "sum(fare_amount)",
        )
        self.add_edge(
            "silver_enriched_trips",
            "trip_distance",
            "gold_zone_daily_metrics",
            "total_distance",
            "sum(trip_distance)",
        )

        # weather_impact_metrics
        self.add_edge(
            "silver_enriched_trips",
            "temp_celsius",
            "gold_weather_impact_metrics",
            "temp_celsius",
            "group_by()",
        )
        self.add_edge(
            "silver_enriched_trips",
            "precipitation_mm",
            "gold_weather_impact_metrics",
            "precipitation_mm",
            "group_by()",
        )
        self.add_edge(
            "silver_enriched_trips",
            "vendor_id",
            "gold_weather_impact_metrics",
            "ride_count",
            "count()",
        )
        self.add_edge(
            "silver_enriched_trips",
            "fare_amount",
            "gold_weather_impact_metrics",
            "avg_fare",
            "avg(fare_amount)",
        )

    def get_upstream_columns(
        self, target_dataset: str, target_column: str
    ) -> list[ColumnLineageEdge]:
        """Query all direct upstream sources that contribute to a target column."""
        return [
            edge
            for edge in self.edges
            if edge.target_dataset == target_dataset and edge.target_column == target_column
        ]

    def get_downstream_columns(
        self, source_dataset: str, source_column: str
    ) -> list[ColumnLineageEdge]:
        """Query all downstream targets impacted by changes to a source column."""
        return [
            edge
            for edge in self.edges
            if edge.source_dataset == source_dataset and edge.source_column == source_column
        ]

    def export_openlineage_event(self, run_id: str, period: str, output_dir: Path) -> Path:
        """Export an OpenLineage-compliant event JSON document to the reports directory."""
        output_dir.mkdir(parents=True, exist_ok=True)
        target_path = output_dir / f"lineage_{period}.json"

        # Group column lineage by target dataset
        fields_by_target: dict[str, dict[str, list[dict[str, str]]]] = {}
        for edge in self.edges:
            target_ds = edge.target_dataset
            if target_ds not in fields_by_target:
                fields_by_target[target_ds] = {}
            if edge.target_column not in fields_by_target[target_ds]:
                fields_by_target[target_ds][edge.target_column] = []
            fields_by_target[target_ds][edge.target_column].append({
                "namespace": "quantaflow",
                "name": edge.source_dataset,
                "field": edge.source_column,
                "transformation": edge.transformation_expr,
            })

        output_datasets: list[dict[str, Any]] = []
        for ds_name, col_map in fields_by_target.items():
            fields_facet = {
                col: {
                    "inputFields": inputs,
                    "transformationDescription": (
                        inputs[0]["transformation"] if inputs else "direct"
                    ),
                }
                for col, inputs in col_map.items()
            }
            output_datasets.append({
                "namespace": "quantaflow",
                "name": ds_name,
                "facets": {
                    "columnLineage": {
                        "_producer": "https://github.com/Ravikishore710/QuantaFlow/src/lineage",
                        "_schemaURL": (
                            "https://openlineage.io/spec/facets/1-0-2/"
                            "ColumnLineageDatasetFacet.json"
                        ),
                        "fields": fields_facet,
                    }
                },
            })

        openlineage_event = {
            "eventType": "COMPLETE",
            "eventTime": datetime.now(UTC).isoformat(),
            "run": {
                "runId": run_id,
                "facets": {
                    "nominalTime": {
                        "nominalStartTime": f"{period}-01T00:00:00Z",
                    }
                },
            },
            "job": {
                "namespace": "quantaflow",
                "name": f"quanta_flow_pipeline_{period}",
            },
            "inputs": [
                {"namespace": "quantaflow", "name": "raw_tlc_yellow"},
                {"namespace": "quantaflow", "name": "raw_noaa_ghcnh"},
                {"namespace": "quantaflow", "name": "raw_taxi_zones"},
            ],
            "outputs": output_datasets,
            "edges": [asdict(e) for e in self.edges],
        }

        target_path.write_text(json.dumps(openlineage_event, indent=2), encoding="utf-8")
        return target_path
