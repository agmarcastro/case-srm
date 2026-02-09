"""
Gold Layer Aggregator - CNPJ Data Extraction Pipeline.

GRAIN: 1 row per (establishment_type, registration_status) combination
(aggregated metrics for business reporting)

Aggregates Silver layer data into business metrics for analytics.
Provides final counts by establishment type and registration status.
"""

import logging
from typing import Optional, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from pipeline_srm.config import PipelineConfig

logger = logging.getLogger(__name__)


class GoldAggregator:
    """
    Aggregates Silver data into Gold layer metrics.

    Responsibilities:
    - Aggregate by establishment_type × registration_status
    - Calculate counts for business reporting
    - Write to Gold layer (single partition for small data)
    - Generate summary reports
    - Query active establishment counts
    """

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[PipelineConfig] = None
    ):
        """
        Initialize Gold Aggregator.

        Args:
            spark: Active SparkSession
            config: Pipeline configuration (uses defaults if None)
        """
        self.spark = spark
        self.config = config or PipelineConfig()

        logger.info("GoldAggregator initialized")

    def aggregate_establishment_counts(
        self,
        silver_df: DataFrame,
        execution_date: str
    ) -> DataFrame:
        """
        Aggregate establishment counts by type and status.

        Performance Strategy:
        - Only count when necessary for validation
        - Cache Silver DataFrame since used multiple times
        - Unpersist after aggregation completes

        Args:
            silver_df: Silver DataFrame
            execution_date: Execution date for partitioning

        Returns:
            DataFrame: Aggregated Gold DataFrame

        Raises:
            ValueError: If silver_df is empty or aggregation validation fails
        """
        if silver_df.rdd.isEmpty():
            raise ValueError("Silver DataFrame is empty")

        # Cache since we'll use Silver multiple times (count + aggregation)
        silver_df.cache()

        try:
            silver_count = silver_df.count()
            logger.info(
                f"Aggregating {silver_count} Silver records for "
                f"execution_date={execution_date}"
            )

            # Aggregate by establishment_type and registration_status
            gold_df = silver_df.groupBy(
                "establishment_type",
                "registration_status"
            ).agg(
                F.count("*").alias("count")
            )

            # Add metadata columns
            gold_df_with_metadata = gold_df \
                .withColumn("dt_ingestion", F.lit(execution_date)) \
                .withColumn("ts_aggregation", F.current_timestamp())

            # Verify aggregation sum matches Silver count
            gold_sum = gold_df_with_metadata.select(
                F.sum("count").alias("total")
            ).collect()[0]["total"]

            if gold_sum != silver_count:
                logger.error(
                    f"Aggregation sum mismatch: Gold sum={gold_sum}, "
                    f"Silver count={silver_count}"
                )
                raise ValueError(
                    f"Aggregation validation failed: sum mismatch "
                    f"({gold_sum} != {silver_count})"
                )

            logger.info(
                f"Aggregation complete: {gold_df_with_metadata.count()} groups, "
                f"sum={gold_sum} (matches Silver)"
            )

            # Write to Gold layer
            self._write_gold(gold_df_with_metadata, execution_date)

            return gold_df_with_metadata

        finally:
            # Unpersist Silver DataFrame
            silver_df.unpersist()

    def _write_gold(self, df: DataFrame, execution_date: str) -> None:
        """
        Write Gold DataFrame to storage.

        Args:
            df: Gold DataFrame to write
            execution_date: Execution date for partitioning
        """
        from pyspark.sql.utils import AnalysisException

        gold_path = self.config.get_gold_path(execution_date)

        logger.info(f"Writing Gold layer to: {gold_path}")

        try:
            # Coalesce to single partition (Gold data is small)
            df.coalesce(1) \
                .write \
                .mode("overwrite") \
                .partitionBy("dt_ingestion") \
                .parquet(self.config.gold_layer_path)

            logger.info(
                f"Gold layer written successfully to {gold_path} "
                f"({df.count()} aggregated records)"
            )

        except AnalysisException as e:
            logger.error(f"Gold layer write failed (schema/path error): {e}")
            raise RuntimeError(
                f"Gold layer write failed. Check path permissions: {gold_path}. Error: {e}"
            ) from e
        except Exception as e:
            logger.error(f"Failed to write Gold layer: {e}")
            raise RuntimeError(f"Gold layer write failed: {e}") from e

    def read_gold_layer(self, execution_date: str) -> DataFrame:
        """
        Read Gold layer data for a specific execution date.

        Args:
            execution_date: Execution date in YYYY-MM-DD format

        Returns:
            DataFrame: Gold layer data

        Raises:
            RuntimeError: If Gold layer cannot be read
        """
        from pyspark.sql.utils import AnalysisException

        gold_path = self.config.get_gold_path(execution_date)

        logger.info(f"Reading Gold layer from: {gold_path}")

        try:
            df = self.spark.read.parquet(gold_path)
            logger.info(f"Successfully read Gold layer ({df.count()} records)")
            return df

        except AnalysisException as e:
            logger.error(f"Gold layer not found or schema error: {e}")
            raise RuntimeError(
                f"Gold layer read failed for {execution_date}. "
                f"Path: {gold_path} may not exist or has schema issues. "
                f"Error: {e}"
            ) from e
        except Exception as e:
            logger.error(f"Failed to read Gold layer: {e}")
            raise RuntimeError(f"Gold layer read failed: {e}") from e

    def get_active_counts(self, execution_date: str) -> DataFrame:
        """
        Query active establishment counts from Gold layer.

        Args:
            execution_date: Execution date to query

        Returns:
            DataFrame: DataFrame with active counts only
        """
        logger.info(f"Querying active counts for {execution_date}")

        # Read Gold layer
        gold_df = self.read_gold_layer(execution_date)

        # Filter for ATIVA status
        active_df = gold_df.filter(
            F.col("registration_status") == "ATIVA"
        )

        active_count = active_df.count()
        logger.info(f"Found {active_count} active status groups")

        return active_df

    def generate_summary_report(self, gold_df: DataFrame) -> Dict[str, any]:
        """
        Generate summary report from Gold data.

        Safety Check:
        - Validates Gold data size before toPandas() to prevent OOM
        - Gold layer should be small (typically <100 rows)
        - Raises error if data exceeds safe threshold

        Args:
            gold_df: Gold DataFrame

        Returns:
            dict: Summary statistics including active counts and ratios

        Raises:
            ValueError: If Gold data exceeds safe size for toPandas()
        """
        logger.info("Generating summary report from Gold data")

        # Validate data size before toPandas() to prevent OOM
        row_count = gold_df.count()

        if row_count > self.config.max_pandas_rows:
            raise ValueError(
                f"Gold aggregation too large for toPandas(): {row_count} rows. "
                f"Maximum allowed: {self.config.max_pandas_rows}. "
                f"This may indicate an aggregation error or missing filters."
            )

        logger.info(f"Converting {row_count} Gold rows to Pandas (within safe threshold)")

        # Convert to Pandas for easy reporting (Gold data is small)
        try:
            pdf = gold_df.toPandas()
        except Exception as e:
            logger.error(f"Failed to convert Gold DataFrame to Pandas: {e}")
            raise RuntimeError(f"toPandas() conversion failed: {e}") from e

        # Filter for active status
        active_pdf = pdf[pdf['registration_status'] == 'ATIVA']

        # Get active counts by type
        matriz_active = active_pdf[
            active_pdf['establishment_type'] == 'Matriz'
        ]['count'].sum()

        filial_active = active_pdf[
            active_pdf['establishment_type'] == 'Filial'
        ]['count'].sum()

        total_active = matriz_active + filial_active

        # Calculate ratio
        ratio = round(filial_active / matriz_active, 2) if matriz_active > 0 else 0

        # Convert datetime columns to ISO strings for JSON serialization
        # (Pandas Timestamp objects are not JSON-serializable)
        datetime_cols = pdf.select_dtypes(include=["datetime", "datetimetz"]).columns
        for col_name in datetime_cols:
            pdf[col_name] = pdf[col_name].dt.strftime("%Y-%m-%dT%H:%M:%S")

        # Build summary report
        summary = {
            "matriz_active": int(matriz_active),
            "filial_active": int(filial_active),
            "total_active": int(total_active),
            "ratio_filial_matriz": ratio,
            "all_status_breakdown": pdf.to_dict('records')
        }

        logger.info(
            f"Summary report: Matriz={matriz_active}, Filial={filial_active}, "
            f"Total Active={total_active}, Ratio={ratio}"
        )

        # Validate ratio is within expected range
        if not (
            self.config.filial_matriz_ratio_min <=
            ratio <=
            self.config.filial_matriz_ratio_max
        ):
            logger.warning(
                f"Filial/Matriz ratio {ratio} outside expected range "
                f"[{self.config.filial_matriz_ratio_min}, "
                f"{self.config.filial_matriz_ratio_max}]"
            )

        return summary

    def compare_with_previous(
        self,
        current_date: str,
        previous_date: str
    ) -> Dict[str, any]:
        """
        Compare current Gold data with previous execution.

        Args:
            current_date: Current execution date
            previous_date: Previous execution date

        Returns:
            dict: Comparison metrics and variance analysis
        """
        logger.info(
            f"Comparing Gold data: {current_date} vs {previous_date}"
        )

        try:
            current_df = self.read_gold_layer(current_date)
            previous_df = self.read_gold_layer(previous_date)

            current_summary = self.generate_summary_report(current_df)
            previous_summary = self.generate_summary_report(previous_df)

            # Calculate variance
            current_total = current_summary['total_active']
            previous_total = previous_summary['total_active']

            variance = (
                (current_total - previous_total) / previous_total
                if previous_total > 0 else 0
            )

            comparison = {
                "current_date": current_date,
                "previous_date": previous_date,
                "current_total": current_total,
                "previous_total": previous_total,
                "variance": round(variance, 4),
                "variance_pct": round(variance * 100, 2),
                "within_threshold": abs(variance) <= self.config.stability_threshold,
                "current_summary": current_summary,
                "previous_summary": previous_summary
            }

            logger.info(
                f"Comparison complete: variance={variance:.2%}, "
                f"threshold={self.config.stability_threshold:.2%}"
            )

            return comparison

        except Exception as e:
            logger.error(f"Failed to compare Gold data: {e}")
            raise
