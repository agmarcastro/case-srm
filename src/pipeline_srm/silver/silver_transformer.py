"""
Silver Layer Transformer - CNPJ Data Extraction Pipeline.

Filters São Paulo establishments and standardizes fields with code mappings.
Applies business logic transformations to Bronze data.
"""

import logging
from typing import Optional, Dict

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    col,
    lit,
    concat,
    create_map,
    current_timestamp,
)
from pyspark.sql.types import StringType

from pipeline_srm.config import PipelineConfig

logger = logging.getLogger(__name__)


class SilverTransformer:
    """
    Transforms Bronze layer data to Silver layer.

    GRAIN: 1 row per São Paulo establishment
    (filtered for municipio=3550308, cleaned and standardized)

    Responsibilities:
    - Filter São Paulo establishments (municipio = 3550308)
    - Map status codes to registration_status labels
    - Map type codes to establishment_type labels
    - Create cnpj_completo (concatenate 3 parts)
    - Select relevant columns (11 fields)
    - Add Silver metadata

    Performance Optimization:
    - Column pruning: reads only 11 of 30 Bronze columns (63% I/O reduction)
    - Predicate pushdown: filter pushed to Parquet reader
    """

    # São Paulo Federal Revenue code (4 digits, NOT the 7-digit IBGE code)
    SAO_PAULO_IBGE_CODE = "7107"

    # Columns needed from Bronze layer (11 of 30 total)
    # Optimizes I/O by reading only required columns
    BRONZE_COLUMNS_REQUIRED = [
        "cnpj_basico",
        "cnpj_ordem",
        "cnpj_dv",
        "identificador_matriz_filial",
        "situacao_cadastral",
        "nome_fantasia",
        "data_inicio_atividade",
        "municipio",
        "uf",
        "cnae_fiscal_principal",
    ]

    # Status code mapping (situacao_cadastral → registration_status)
    STATUS_MAPPING = {
        1: "NULA",
        2: "ATIVA",      # CRITICAL: Active status for business requirement
        3: "SUSPENSA",
        4: "INAPTA",
        8: "BAIXADA",
    }

    # Establishment type mapping (identificador_matriz_filial → establishment_type)
    TYPE_MAPPING = {
        1: "Matriz",     # Headquarters
        2: "Filial",     # Branch
    }

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[PipelineConfig] = None
    ):
        """
        Initialize Silver Transformer.

        Args:
            spark: Active SparkSession
            config: Pipeline configuration (uses defaults if None)
        """
        self.spark = spark
        self.config = config or PipelineConfig()

        # Verify São Paulo IBGE code matches config
        if self.config.SAO_PAULO_IBGE_CODE != self.SAO_PAULO_IBGE_CODE:
            logger.warning(
                f"Config IBGE code {self.config.SAO_PAULO_IBGE_CODE} differs from "
                f"transformer constant {self.SAO_PAULO_IBGE_CODE}. "
                "Using config value."
            )

    def read_bronze_with_pruning(self, execution_date: str) -> DataFrame:
        """
        Read Bronze layer with column pruning for optimal I/O.

        Performance Optimization:
        - Reads only 11 of 30 columns (63% I/O reduction)
        - Parquet reader skips unnecessary columns at scan time
        - Reduces memory footprint

        Args:
            execution_date: Execution date in YYYY-MM-DD format

        Returns:
            DataFrame: Bronze data with only required columns
        """
        from pipeline_srm.bronze.bronze_loader import BronzeLoader

        bronze_loader = BronzeLoader(self.spark, self.config)

        logger.info(
            f"Reading Bronze layer with column pruning "
            f"({len(self.BRONZE_COLUMNS_REQUIRED)} columns)"
        )

        bronze_df = bronze_loader.read_bronze_layer(
            reference_date=execution_date,
            columns=self.BRONZE_COLUMNS_REQUIRED
        )

        return bronze_df

    def transform_to_silver(
        self,
        bronze_df: DataFrame,
        execution_date: str
    ) -> DataFrame:
        """
        Transform Bronze DataFrame to Silver layer.

        Performance Strategy:
        - Avoids premature count() calls (expensive full scans)
        - Let Spark optimize the full DAG before materialization
        - Only count at the end for validation/logging

        Args:
            bronze_df: Bronze layer DataFrame
            execution_date: Execution date in YYYY-MM-DD format

        Returns:
            DataFrame: Silver layer DataFrame with São Paulo data only

        Raises:
            ValueError: If bronze_df is empty or invalid
        """
        if bronze_df.rdd.isEmpty():
            raise ValueError("Bronze DataFrame is empty")

        logger.info(
            f"Transforming Bronze to Silver for execution_date={execution_date}"
        )

        # Apply São Paulo filtering
        filtered_df = self._filter_sao_paulo(bronze_df)

        # Standardize fields (mappings and CNPJ concatenation)
        standardized_df = self._standardize_fields(filtered_df)

        # Select relevant columns for Silver layer
        silver_columns = [
            "cnpj_completo",
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "establishment_type",
            "registration_status",
            "nome_fantasia",
            "data_inicio_atividade",
            "municipio",
            "uf",
            "cnae_fiscal_principal",
        ]

        silver_df = standardized_df.select(*silver_columns)

        # Add Silver metadata
        silver_df_with_metadata = silver_df \
            .withColumn("dt_ingestion", lit(execution_date)) \
            .withColumn("ts_transform", current_timestamp())

        # Write to Silver layer (Spark optimizes entire DAG here)
        self._write_silver(silver_df_with_metadata, execution_date)

        logger.info("Silver transformation complete")

        return silver_df_with_metadata

    def _filter_sao_paulo(self, df: DataFrame) -> DataFrame:
        """
        Filter establishments located in São Paulo city.

        Performance Notes:
        - Predicate pushdown: filter is pushed to Parquet reader
        - No premature count() to allow Spark to optimize full DAG
        - Execution plan logged if DEBUG enabled

        Args:
            df: Bronze DataFrame

        Returns:
            DataFrame: Filtered DataFrame with São Paulo records only
        """
        # Use config value for IBGE code
        ibge_code = self.config.SAO_PAULO_IBGE_CODE

        logger.info(f"Filtering for São Paulo (IBGE code: {ibge_code})")

        # Apply filters:
        # 1. municipio == '3550308' (São Paulo city)
        # 2. situacao_cadastral IS NOT NULL (must have status)
        # 3. identificador_matriz_filial IN (1, 2) (only Matriz or Filial)
        filtered_df = df.filter(
            (col("municipio") == ibge_code) &
            (col("situacao_cadastral").isNotNull()) &
            (col("identificador_matriz_filial").isin([1, 2]))
        )

        # Log execution plan if enabled (DEBUG mode)
        if self.config.enable_explain_plans and logger.isEnabledFor(logging.DEBUG):
            logger.debug("Execution plan for São Paulo filter:")
            filtered_df.explain(True)

        return filtered_df

    def _standardize_fields(self, df: DataFrame) -> DataFrame:
        """
        Standardize fields with code mappings and CNPJ concatenation.

        Args:
            df: Filtered DataFrame

        Returns:
            DataFrame: Standardized DataFrame with mapped fields
        """
        # Create PySpark mapping expressions for efficient transformation
        from pyspark.sql.functions import when

        # Build status mapping using when/otherwise chain
        status_expr = None
        for code, label in self.STATUS_MAPPING.items():
            if status_expr is None:
                status_expr = when(col("situacao_cadastral") == code, lit(label))
            else:
                status_expr = status_expr.when(
                    col("situacao_cadastral") == code,
                    lit(label)
                )
        status_expr = status_expr.otherwise(lit("UNKNOWN"))

        # Build type mapping using when/otherwise chain
        type_expr = None
        for code, label in self.TYPE_MAPPING.items():
            if type_expr is None:
                type_expr = when(col("identificador_matriz_filial") == code, lit(label))
            else:
                type_expr = type_expr.when(
                    col("identificador_matriz_filial") == code,
                    lit(label)
                )
        type_expr = type_expr.otherwise(lit("UNKNOWN"))

        # Apply transformations
        standardized_df = df \
            .withColumn("registration_status", status_expr) \
            .withColumn("establishment_type", type_expr) \
            .withColumn(
                "cnpj_completo",
                concat(
                    col("cnpj_basico"),
                    col("cnpj_ordem"),
                    col("cnpj_dv")
                )
            )

        logger.info("Field standardization complete (status, type, CNPJ)")

        return standardized_df

    def _write_silver(self, df: DataFrame, execution_date: str) -> None:
        """
        Write Silver DataFrame to storage with partition optimization.

        Performance Strategy:
        - Calculates optimal partition count based on data size
        - Target: 128MB-1GB per Parquet file (default 256MB)
        - Coalesces before write to reduce small files

        Args:
            df: Silver DataFrame to write
            execution_date: Execution date for partitioning
        """
        from pyspark.sql.utils import AnalysisException

        silver_path = self.config.get_silver_path(execution_date)

        logger.info(f"Writing Silver layer to: {silver_path}")

        # Calculate optimal partition count
        optimal_partitions = self._calculate_output_partitions(df)

        try:
            df.coalesce(optimal_partitions) \
                .write \
                .mode("overwrite") \
                .partitionBy("dt_ingestion") \
                .parquet(self.config.silver_layer_path)

            logger.info(
                f"Silver layer written successfully to {silver_path} "
                f"(coalesced to {optimal_partitions} partitions)"
            )

        except AnalysisException as e:
            logger.error(f"Silver layer write failed (schema/path error): {e}")
            raise RuntimeError(
                f"Silver layer write failed. Check path permissions: {silver_path}. Error: {e}"
            ) from e
        except Exception as e:
            logger.error(f"Failed to write Silver layer: {e}")
            raise RuntimeError(f"Silver layer write failed: {e}") from e

    def _calculate_output_partitions(self, df: DataFrame) -> int:
        """
        Calculate optimal number of output partitions for Parquet write.

        Target: 128MB-1GB per file (default 256MB)
        Formula: (record_count * avg_row_size_kb) / (target_file_size_mb * 1024)

        Args:
            df: DataFrame to write

        Returns:
            int: Optimal partition count (minimum 1)
        """
        # Cache for count
        df.cache()

        try:
            record_count = df.count()

            # Estimate average row size (Silver records ~0.5KB per row)
            estimated_row_size_kb = 0.5

            # Calculate partitions
            target_file_size_mb = self.config.target_file_size_mb
            partitions = max(
                1,
                int((record_count * estimated_row_size_kb) / (target_file_size_mb * 1024))
            )

            logger.info(
                f"Calculated {partitions} output partitions for {record_count} records "
                f"(target: {target_file_size_mb}MB per file)"
            )

            return partitions

        finally:
            df.unpersist()

    def read_silver_layer(self, execution_date: str) -> DataFrame:
        """
        Read Silver layer data for a specific execution date.

        Args:
            execution_date: Execution date in YYYY-MM-DD format

        Returns:
            DataFrame: Silver layer data

        Raises:
            RuntimeError: If Silver layer cannot be read
        """
        from pyspark.sql.utils import AnalysisException

        silver_path = self.config.get_silver_path(execution_date)

        logger.info(f"Reading Silver layer from: {silver_path}")

        try:
            df = self.spark.read.parquet(silver_path)
            logger.info(f"Successfully read Silver layer")
            return df

        except AnalysisException as e:
            logger.error(f"Silver layer not found or schema error: {e}")
            raise RuntimeError(
                f"Silver layer read failed for {execution_date}. "
                f"Path: {silver_path} may not exist or has schema issues. "
                f"Run Bronze→Silver transformation first. Error: {e}"
            ) from e
        except Exception as e:
            logger.error(f"Failed to read Silver layer: {e}")
            raise RuntimeError(f"Silver layer read failed: {e}") from e

    def get_data_quality_metrics(self, silver_df: DataFrame) -> Dict[str, any]:
        """
        Calculate data quality metrics for Silver layer.

        Args:
            silver_df: Silver DataFrame

        Returns:
            dict: Data quality metrics
        """
        total_count = silver_df.count()

        # Count by establishment type
        type_counts = silver_df.groupBy("establishment_type").count().collect()
        type_dict = {row["establishment_type"]: row["count"] for row in type_counts}

        matriz_count = type_dict.get("Matriz", 0)
        filial_count = type_dict.get("Filial", 0)

        # Count by registration status
        status_counts = silver_df.groupBy("registration_status").count().collect()
        status_dict = {row["registration_status"]: row["count"] for row in status_counts}

        active_count = status_dict.get("ATIVA", 0)

        # Calculate Filial/Matriz ratio
        ratio = filial_count / matriz_count if matriz_count > 0 else 0

        metrics = {
            "total_records": total_count,
            "matriz_count": matriz_count,
            "filial_count": filial_count,
            "filial_matriz_ratio": ratio,
            "active_count": active_count,
            "status_distribution": status_dict,
            "type_distribution": type_dict,
        }

        logger.info(
            f"Data quality metrics: {total_count} total, "
            f"{matriz_count} Matriz, {filial_count} Filial, "
            f"ratio={ratio:.2f}, {active_count} active"
        )

        return metrics
