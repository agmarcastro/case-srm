"""
Bronze Layer Loader - CNPJ Data Extraction Pipeline.

GRAIN: 1 row per establishment record from source CSV file
(preserves exact source fidelity with metadata)

Loads raw CNPJ CSV data into Bronze layer as Parquet with schema validation.
Handles ISO-8859-1 encoding and corrupt record detection.
"""

import logging
from pathlib import Path
from typing import List, Optional
from datetime import datetime

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    IntegerType,
)

from pipeline_srm.config import PipelineConfig, SparkConfig

logger = logging.getLogger(__name__)


class BronzeLoader:
    """
    Loads raw CNPJ data into Bronze layer.

    Responsibilities:
    - Read CSV files with explicit schema (no inference)
    - Handle ISO-8859-1 encoding
    - Validate schema structure
    - Add ingestion metadata columns
    - Write Parquet with Snappy compression
    - Partition by dt_ingestion (YYYY-MM-DD)
    """

    # CNPJ Estabelecimentos schema (30 fields total)
    # Based on Federal Revenue data specification
    ESTABELECIMENTOS_SCHEMA = StructType([
        StructField("cnpj_basico", StringType(), False),  # 8 digits - REQUIRED
        StructField("cnpj_ordem", StringType(), False),   # 4 digits - REQUIRED
        StructField("cnpj_dv", StringType(), False),      # 2 digits - REQUIRED
        StructField("identificador_matriz_filial", IntegerType(), True),  # 1=Matriz, 2=Filial
        StructField("nome_fantasia", StringType(), True),
        StructField("situacao_cadastral", IntegerType(), True),  # 1,2,3,4,8
        StructField("data_situacao_cadastral", StringType(), True),
        StructField("motivo_situacao_cadastral", IntegerType(), True),
        StructField("nome_cidade_exterior", StringType(), True),
        StructField("pais", StringType(), True),
        StructField("data_inicio_atividade", StringType(), True),
        StructField("cnae_fiscal_principal", StringType(), True),
        StructField("cnae_fiscal_secundaria", StringType(), True),
        StructField("tipo_logradouro", StringType(), True),
        StructField("logradouro", StringType(), True),
        StructField("numero", StringType(), True),
        StructField("complemento", StringType(), True),
        StructField("bairro", StringType(), True),
        StructField("cep", StringType(), True),
        StructField("uf", StringType(), True),
        StructField("municipio", StringType(), True),  # CRITICAL: IBGE code (7 digits)
        StructField("ddd_1", StringType(), True),
        StructField("telefone_1", StringType(), True),
        StructField("ddd_2", StringType(), True),
        StructField("telefone_2", StringType(), True),
        StructField("ddd_fax", StringType(), True),
        StructField("fax", StringType(), True),
        StructField("correio_eletronico", StringType(), True),
        StructField("situacao_especial", StringType(), True),
        StructField("data_situacao_especial", StringType(), True),
    ])

    def __init__(
        self,
        spark: SparkSession,
        config: Optional[PipelineConfig] = None
    ):
        """
        Initialize Bronze Loader.

        Args:
            spark: Active SparkSession
            config: Pipeline configuration (uses defaults if None)
        """
        self.spark = spark
        self.config = config or PipelineConfig()
        self._configure_spark()

    def _configure_spark(self):
        """Configure Spark session for Bronze layer processing."""
        # Set compression codec
        self.spark.conf.set(
            "spark.sql.parquet.compression.codec",
            "snappy"
        )

        logger.info("Spark configured for Bronze layer processing")

    def load_csv_to_bronze(
        self,
        csv_paths: List[Path],
        reference_date: str
    ) -> DataFrame:
        """
        Load CSV files to Bronze layer as Parquet.

        Args:
            csv_paths: List of paths to CSV files
            reference_date: Reference date in YYYY-MM or YYYY-MM-DD format

        Returns:
            DataFrame: Loaded Bronze DataFrame with metadata

        Raises:
            ValueError: If no CSV files provided or invalid date format
            RuntimeError: If CSV loading fails
        """
        if not csv_paths:
            raise ValueError("No CSV files provided to load")

        # Validate date format (accept both YYYY-MM and YYYY-MM-DD)
        date_formats = ["%Y-%m", "%Y-%m-%d"]
        valid_format = False
        for fmt in date_formats:
            try:
                datetime.strptime(reference_date, fmt)
                valid_format = True
                break
            except ValueError:
                continue

        if not valid_format:
            raise ValueError(
                f"Invalid reference_date format: {reference_date}. "
                "Expected YYYY-MM or YYYY-MM-DD"
            )

        logger.info(
            f"Loading {len(csv_paths)} CSV files to Bronze layer "
            f"for reference_date={reference_date}"
        )

        # Convert Path objects to strings
        csv_path_strings = [str(p) for p in csv_paths]

        # Read CSV with explicit schema and encoding handling
        try:
            from pyspark.sql.utils import AnalysisException, ParseException

            df = self.spark.read.format("csv") \
                .option("delimiter", self.config.csv_delimiter) \
                .option("encoding", self.config.csv_encoding) \
                .option("header", str(self.config.csv_header).lower()) \
                .option("mode", self.config.csv_mode) \
                .option("columnNameOfCorruptRecord", "_corrupt_record") \
                .schema(self.ESTABELECIMENTOS_SCHEMA) \
                .load(csv_path_strings)

            logger.info("Successfully loaded CSV files to DataFrame")

        except ParseException as e:
            logger.error(f"CSV parse error: {e}")
            raise ValueError(
                f"Check CSV encoding/delimiter configuration. "
                f"Expected encoding={self.config.csv_encoding}, "
                f"delimiter='{self.config.csv_delimiter}'. Error: {e}"
            ) from e
        except AnalysisException as e:
            logger.error(f"Schema or file analysis error: {e}")
            raise ValueError(
                f"Schema mismatch or files not found at paths: {csv_path_strings}. "
                f"Error: {e}"
            ) from e
        except Exception as e:
            logger.error(f"Failed to read CSV files: {e}")
            raise RuntimeError(f"CSV loading failed: {e}") from e

        # Validate schema
        self._validate_schema(df)

        # Validate corrupt records BEFORE transformations
        self._validate_corrupt_records(df)

        # Add ingestion metadata
        from pyspark.sql.functions import lit, current_timestamp

        df_with_metadata = df \
            .withColumn("dt_ingestion", lit(reference_date)) \
            .withColumn("ts_ingestion", current_timestamp())

        # Calculate optimal partition count for write
        # Target: 128MB-1GB per Parquet file (using 256MB default)
        optimal_partitions = self._calculate_output_partitions(df_with_metadata)

        # Write to Bronze layer (partitioned by dt_ingestion)
        bronze_path = self.config.get_bronze_path(reference_date)

        logger.info(f"Writing Bronze layer to: {bronze_path}")

        try:
            from pyspark.sql.utils import AnalysisException

            # Coalesce to optimal partition count before write
            df_with_metadata.coalesce(optimal_partitions) \
                .write \
                .mode("overwrite") \
                .partitionBy("dt_ingestion") \
                .parquet(self.config.bronze_layer_path)

            logger.info(
                f"Successfully wrote Bronze layer to {bronze_path} "
                f"(coalesced to {optimal_partitions} partitions)"
            )

        except AnalysisException as e:
            logger.error(f"Failed to write Bronze layer (schema/path error): {e}")
            raise RuntimeError(
                f"Bronze layer write failed. Check path permissions and schema. Error: {e}"
            ) from e
        except Exception as e:
            logger.error(f"Failed to write Bronze layer: {e}")
            raise RuntimeError(f"Bronze layer write failed: {e}") from e

        return df_with_metadata

    def _validate_corrupt_records(self, df: DataFrame) -> None:
        """
        Validate corrupt record rate is within acceptable threshold.

        Corrupt records are captured in _corrupt_record column by PERMISSIVE mode.
        If rate exceeds threshold, raises error to prevent data quality issues.

        Args:
            df: DataFrame with potential _corrupt_record column

        Raises:
            ValueError: If corrupt record rate exceeds max_corrupt_rate threshold
        """
        from pyspark.sql.functions import col

        # Check if corrupt record column exists
        if "_corrupt_record" not in df.columns:
            logger.info("No corrupt record column found - all records parsed successfully")
            return

        # Cache DataFrame since we'll count twice
        df.cache()

        try:
            total_count = df.count()
            corrupt_count = df.filter(col("_corrupt_record").isNotNull()).count()

            if corrupt_count == 0:
                logger.info("No corrupt records detected")
                return

            corrupt_rate = corrupt_count / total_count if total_count > 0 else 0

            logger.warning(
                f"Found {corrupt_count} corrupt records out of {total_count} "
                f"({corrupt_rate:.2%} corrupt rate)"
            )

            # Check against threshold
            if corrupt_rate > self.config.max_corrupt_rate:
                raise ValueError(
                    f"Corrupt record rate too high: {corrupt_rate:.2%} "
                    f"(threshold: {self.config.max_corrupt_rate:.2%}). "
                    f"Check CSV encoding ({self.config.csv_encoding}) and "
                    f"delimiter ('{self.config.csv_delimiter}')."
                )

        finally:
            df.unpersist()

    def _calculate_output_partitions(self, df: DataFrame) -> int:
        """
        Calculate optimal number of output partitions for Parquet write.

        Uses input partition count as a proxy to avoid an expensive count()
        that can OOM the JVM in memory-constrained environments.

        Heuristic: Each input CSV partition is ~200-400MB uncompressed.
        For a 256MB Parquet target, a 1:1 ratio is a reasonable starting point.
        AQE will further coalesce at runtime if partitions are too small.

        Args:
            df: DataFrame to write

        Returns:
            int: Optimal partition count (minimum 1)
        """
        input_partitions = df.rdd.getNumPartitions()

        # Use input partitions as proxy (each CSV split ≈ 200-400MB)
        # For 256MB Parquet target, keep roughly the same count
        target_file_size_mb = self.config.target_file_size_mb
        partitions = max(1, input_partitions)

        logger.info(
            f"Calculated {partitions} output partitions from {input_partitions} input partitions "
            f"(target: {target_file_size_mb}MB per file, AQE will optimize at runtime)"
        )

        return partitions

    def _validate_schema(self, df: DataFrame) -> None:
        """
        Validate DataFrame schema matches expected ESTABELECIMENTOS schema.

        Args:
            df: DataFrame to validate

        Raises:
            ValueError: If schema doesn't match expected structure
        """
        expected_fields = {field.name for field in self.ESTABELECIMENTOS_SCHEMA.fields}
        actual_fields = {field.name for field in df.schema.fields}

        # Check for missing fields
        missing = expected_fields - actual_fields
        if missing:
            raise ValueError(
                f"Schema validation failed. Missing fields: {missing}"
            )

        # Check for extra fields (excluding corrupt record column)
        extra = actual_fields - expected_fields - {"_corrupt_record"}
        if extra:
            raise ValueError(
                f"Schema validation failed. Unexpected fields: {extra}"
            )

        # Validate critical non-nullable fields
        critical_fields = ["cnpj_basico", "cnpj_ordem", "cnpj_dv"]
        for field_name in critical_fields:
            field = next(f for f in df.schema.fields if f.name == field_name)
            if field.nullable:
                logger.warning(
                    f"Critical field '{field_name}' is nullable in actual data"
                )

        logger.info("Schema validation passed - all 30 fields present")

    def read_bronze_layer(
        self,
        reference_date: str,
        columns: Optional[List[str]] = None
    ) -> DataFrame:
        """
        Read Bronze layer data for a specific reference date.

        Column Pruning:
        - If columns specified, reads only those columns (performance optimization)
        - Reduces I/O by avoiding reading unnecessary columns
        - Recommended: specify only columns needed for downstream processing

        Args:
            reference_date: Reference date in YYYY-MM or YYYY-MM-DD format
            columns: Optional list of column names to read (None = all columns)

        Returns:
            DataFrame: Bronze layer data
        """
        bronze_path = self.config.get_bronze_path(reference_date)

        logger.info(f"Reading Bronze layer from: {bronze_path}")

        try:
            df = self.spark.read.parquet(bronze_path)

            # Apply column pruning if specified
            if columns:
                # Validate requested columns exist in schema
                available_columns = set(df.columns)
                requested_columns = set(columns)
                missing_columns = requested_columns - available_columns

                if missing_columns:
                    logger.warning(
                        f"Requested columns not found in Bronze schema: {missing_columns}"
                    )

                # Select only requested columns that exist
                valid_columns = [col for col in columns if col in available_columns]
                df = df.select(*valid_columns)

                logger.info(
                    f"Column pruning applied: reading {len(valid_columns)}/{len(available_columns)} columns"
                )

            logger.info(f"Successfully read Bronze layer ({len(df.columns)} columns)")
            return df

        except Exception as e:
            logger.error(f"Failed to read Bronze layer: {e}")
            raise RuntimeError(f"Bronze layer read failed: {e}")

    def get_schema_info(self) -> dict:
        """
        Get information about the ESTABELECIMENTOS schema.

        Returns:
            dict: Schema metadata including field count and types
        """
        schema_info = {
            "total_fields": len(self.ESTABELECIMENTOS_SCHEMA.fields),
            "required_fields": [
                f.name for f in self.ESTABELECIMENTOS_SCHEMA.fields
                if not f.nullable
            ],
            "critical_fields": ["cnpj_basico", "cnpj_ordem", "cnpj_dv", "municipio"],
            "fields": [
                {
                    "name": f.name,
                    "type": str(f.dataType),
                    "nullable": f.nullable
                }
                for f in self.ESTABELECIMENTOS_SCHEMA.fields
            ]
        }

        return schema_info
