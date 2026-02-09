"""
Configuration management for CNPJ Data Extraction Pipeline.

Provides dataclasses for Download, Spark, and Pipeline configuration
with support for environment variable overrides.
"""

import os
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import List, Optional


class FileType(Enum):
    """
    Enumeration of CNPJ file types available from Federal Revenue.

    The Federal Revenue provides different file types:
    - ESTABELECIMENTOS: Indexed files (0-9), ~600MB each compressed
    - MUNICIPIOS: Single file with city codes and names
    """
    ESTABELECIMENTOS = "estabelecimentos"
    MUNICIPIOS = "municipios"


@dataclass
class FileTypeConfig:
    """
    Configuration for a specific CNPJ file type.

    Attributes:
        pattern: Filename pattern (use {} for indexed files)
        indices: List of file indices (None for single files like Municipios)
        enabled: Whether to download this file type
    """
    pattern: str
    indices: Optional[List[int]] = None
    enabled: bool = True


@dataclass
class DownloadConfig:
    """Configuration for CNPJ data download from Federal Revenue.

    The Federal Revenue provides CNPJ data via a Nextcloud public share.
    Files are organized in monthly directories (YYYY-MM) containing:
    - Estabelecimentos0.zip through Estabelecimentos9.zip (indexed)
    - Municipios.zip (single file with city codes)

    Download URL format - Nextcloud WebDAV endpoint:
    https://arquivos.receitafederal.gov.br/public.php/webdav/{YYYY-MM}/{filename}

    Authentication: Basic auth with share token as username, empty password
    """

    # Federal Revenue Nextcloud public share token
    # This is the official public share for CNPJ open data
    share_token: str = "YggdBLfdninEJX9"
    webdav_base_url: str = "https://arquivos.receitafederal.gov.br/public.php/webdav"
    # Changed from /tmp to /opt/data for shared volume access across Airflow and Spark
    download_dir: Path = field(default_factory=lambda: Path("/opt/data/raw"))
    timeout_seconds: int = 600  # Increased timeout for large files (~1.5GB each)
    chunk_size: int = 1048576  # 1MB chunks for faster downloads

    # File type configurations
    file_types: dict = field(default_factory=lambda: {
        FileType.ESTABELECIMENTOS: FileTypeConfig(
            pattern="Estabelecimentos{}.zip",
            indices=list(range(10)),  # 0-9
            enabled=True
        ),
        FileType.MUNICIPIOS: FileTypeConfig(
            pattern="Municipios.zip",
            indices=None,  # Single file, no index
            enabled=True
        ),
    })

    # Legacy compatibility - will be deprecated
    file_pattern: str = "Estabelecimentos{}.zip"
    file_indices: Optional[List[int]] = None

    def __post_init__(self):
        """Override from environment variables if present."""
        if env_token := os.getenv("CNPJ_SHARE_TOKEN"):
            self.share_token = env_token
        if env_webdav := os.getenv("CNPJ_WEBDAV_URL"):
            self.webdav_base_url = env_webdav
        if env_dir := os.getenv("CNPJ_DOWNLOAD_DIR"):
            self.download_dir = Path(env_dir)
        if env_timeout := os.getenv("CNPJ_TIMEOUT"):
            self.timeout_seconds = int(env_timeout)

        # Ensure download directory exists
        self.download_dir.mkdir(parents=True, exist_ok=True)

        # Set default file indices if not provided (legacy compatibility)
        if self.file_indices is None:
            self.file_indices = list(range(10))  # 0-9

    def get_download_url(self, reference_month: str, filename: str) -> str:
        """
        Build Nextcloud WebDAV download URL.

        Args:
            reference_month: Reference month in YYYY-MM format
            filename: File to download (e.g., Estabelecimentos0.zip)

        Returns:
            Complete WebDAV URL for the file
        """
        # Nextcloud WebDAV format:
        # {webdav_base_url}/{YYYY-MM}/{filename}
        return f"{self.webdav_base_url}/{reference_month}/{filename}"

    def get_files_to_download(self, file_type: FileType) -> List[str]:
        """
        Get list of filenames to download for a given file type.

        Args:
            file_type: The type of files to get (ESTABELECIMENTOS or MUNICIPIOS)

        Returns:
            List of filenames to download
        """
        config = self.file_types.get(file_type)
        if not config or not config.enabled:
            return []

        if config.indices is None:
            # Single file (e.g., Municipios.zip)
            return [config.pattern]
        else:
            # Indexed files (e.g., Estabelecimentos0.zip through Estabelecimentos9.zip)
            return [config.pattern.format(i) for i in config.indices]

    def get_enabled_file_types(self) -> List[FileType]:
        """Get list of enabled file types for download."""
        return [ft for ft, cfg in self.file_types.items() if cfg.enabled]


@dataclass
class SparkConfig:
    """
    Configuration for PySpark session and execution.

    Performance Tuning Strategy:
    - AQE (Adaptive Query Execution): Dynamically optimizes query plans at runtime
    - Shuffle Partitions: Calculated as executor_cores * executor_instances * 2 (or cpu_count * 4)
    - Memory Fraction: 80% for execution + storage, 30% of that reserved for storage
    """

    app_name: str = "CNPJ-Pipeline-SRM"
    executor_memory: str = "1g"  # Reduced for Docker workers (2GB RAM each)
    driver_memory: str = "512m"  # Must fit inside scheduler container alongside Airflow (~300MB)
    executor_cores: int = 2     # Use all cores per executor (2 per worker)
    executor_instances: int = 2  # Fixed to 2 (one per worker) to prevent OOM

    # Shuffle partitions - calculated dynamically if not overridden
    shuffle_partitions: Optional[int] = None
    default_parallelism: int = 100

    # Adaptive Query Execution (AQE) settings
    aqe_enabled: bool = True
    aqe_coalesce_partitions: bool = True
    aqe_skew_join: bool = True

    # Memory configuration
    memory_fraction: float = 0.8  # 80% for execution + storage
    storage_fraction: float = 0.3  # 30% of execution memory for storage

    # Compression settings
    parquet_compression: str = "snappy"

    # Performance monitoring
    event_log_enabled: bool = True
    event_log_dir: Path = field(default_factory=lambda: Path("/tmp/spark-events"))

    # S3/MinIO settings (optional)
    s3_endpoint: Optional[str] = None
    s3_access_key: Optional[str] = None
    s3_secret_key: Optional[str] = None
    s3_path_style_access: bool = True

    def __post_init__(self):
        """
        Override from environment variables if present.
        Calculate optimal shuffle partitions if not specified.
        """
        if env_memory := os.getenv("SPARK_EXECUTOR_MEMORY"):
            self.executor_memory = env_memory
        if env_driver := os.getenv("SPARK_DRIVER_MEMORY"):
            self.driver_memory = env_driver
        if env_partitions := os.getenv("SPARK_SHUFFLE_PARTITIONS"):
            self.shuffle_partitions = int(env_partitions)

        # AQE environment overrides
        if env_aqe := os.getenv("SPARK_AQE_ENABLED"):
            self.aqe_enabled = env_aqe.lower() in ("true", "1", "yes")
        if env_aqe_coalesce := os.getenv("SPARK_AQE_COALESCE_PARTITIONS"):
            self.aqe_coalesce_partitions = env_aqe_coalesce.lower() in ("true", "1", "yes")
        if env_aqe_skew := os.getenv("SPARK_AQE_SKEW_JOIN"):
            self.aqe_skew_join = env_aqe_skew.lower() in ("true", "1", "yes")

        # Calculate optimal shuffle partitions if not specified
        # Formula: executor_cores * executor_instances * 2
        # Fallback for local mode: cpu_count * 4
        if self.shuffle_partitions is None:
            if self.executor_instances > 1:
                # Distributed mode
                self.shuffle_partitions = self.executor_cores * self.executor_instances * 2
            else:
                # Local mode - use CPU count
                import os as os_module
                cpu_count = os_module.cpu_count() or 4
                self.shuffle_partitions = cpu_count * 4

        # Event log configuration
        if env_event_log := os.getenv("SPARK_EVENT_LOG_DIR"):
            self.event_log_dir = Path(env_event_log)

        # Create event log directory if enabled
        if self.event_log_enabled:
            self.event_log_dir.mkdir(parents=True, exist_ok=True)

        # S3/MinIO configuration
        if env_endpoint := os.getenv("S3_ENDPOINT"):
            self.s3_endpoint = env_endpoint
        if env_access := os.getenv("S3_ACCESS_KEY"):
            self.s3_access_key = env_access
        if env_secret := os.getenv("S3_SECRET_KEY"):
            self.s3_secret_key = env_secret

    def get_spark_config_dict(self) -> dict:
        """
        Return configuration as dictionary for SparkSession builder.

        Includes:
        - Basic executor/driver settings
        - Adaptive Query Execution (AQE) for runtime optimization
        - Memory tuning (execution + storage fractions)
        - Event logging for performance monitoring
        - Calculated shuffle partitions
        """
        # Calculate memory overhead (10% of executor memory)
        executor_mem_gb = int(self.executor_memory.replace('g', '').replace('G', ''))
        memory_overhead = max(384, int(executor_mem_gb * 1024 * 0.1))  # At least 384MB

        config = {
            # Basic configuration
            "spark.app.name": self.app_name,
            "spark.executor.memory": self.executor_memory,
            "spark.driver.memory": self.driver_memory,
            "spark.executor.cores": str(self.executor_cores),
            "spark.executor.memoryOverhead": f"{memory_overhead}m",

            # Driver network configuration (critical for Docker/container deployments)
            # Workers must be able to connect back to the driver
            "spark.driver.host": os.getenv("SPARK_DRIVER_HOST", "airflow-scheduler"),
            "spark.driver.bindAddress": "0.0.0.0",

            # Executor instance control (prevents OOM by limiting executors per worker)
            # With 2GB workers, limit to 1 executor per worker to avoid memory exhaustion
            "spark.executor.instances": str(self.executor_instances),
            "spark.dynamicAllocation.enabled": "false",

            # Shuffle and parallelism
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.default.parallelism": str(self.default_parallelism),

            # Adaptive Query Execution (AQE)
            "spark.sql.adaptive.enabled": str(self.aqe_enabled).lower(),
            "spark.sql.adaptive.coalescePartitions.enabled": str(self.aqe_coalesce_partitions).lower(),
            "spark.sql.adaptive.skewJoin.enabled": str(self.aqe_skew_join).lower(),

            # Memory configuration
            "spark.memory.fraction": str(self.memory_fraction),
            "spark.memory.storageFraction": str(self.storage_fraction),

            # Compression
            "spark.sql.parquet.compression.codec": self.parquet_compression,

            # Python version alignment (driver and executor must match)
            "spark.pyspark.python": "python3.11",
            "spark.pyspark.driver.python": "python3.11",

            # Event logging for monitoring
            "spark.eventLog.enabled": str(self.event_log_enabled).lower(),
            "spark.eventLog.dir": str(self.event_log_dir),
        }

        # Add S3/MinIO configuration if provided
        if self.s3_endpoint:
            config.update({
                "spark.hadoop.fs.s3a.endpoint": self.s3_endpoint,
                "spark.hadoop.fs.s3a.access.key": self.s3_access_key or "",
                "spark.hadoop.fs.s3a.secret.key": self.s3_secret_key or "",
                "spark.hadoop.fs.s3a.path.style.access": str(self.s3_path_style_access).lower(),
                "spark.hadoop.fs.s3a.impl": "org.apache.hadoop.fs.s3a.S3AFileSystem",
            })

        return config


@dataclass
class PipelineConfig:
    """Configuration for pipeline layers and business logic."""

    # Critical business constants
    SAO_PAULO_IBGE_CODE: str = "7107"  # São Paulo city code in Federal Revenue system (4 digits)
    MATRIZ_CODE: int = 1  # Headquarters
    FILIAL_CODE: int = 2  # Branch
    ACTIVE_STATUS_CODE: int = 2  # Active registration status

    # Layer paths (S3A URIs for MinIO/S3, or local filesystem paths)
    bronze_layer_path: str = "s3a://cnpj-data/bronze"
    silver_layer_path: str = "s3a://cnpj-data/silver"
    gold_layer_path: str = "s3a://cnpj-data/gold"

    # CSV parsing settings
    csv_delimiter: str = ";"
    csv_encoding: str = "ISO-8859-1"
    csv_header: bool = False
    csv_mode: str = "PERMISSIVE"  # Captures corrupt records

    # Error handling thresholds
    max_corrupt_rate: float = 0.01  # Maximum 1% corrupt record rate
    max_pandas_rows: int = 10_000  # Maximum rows to collect with toPandas()

    # Performance tuning
    target_file_size_mb: int = 256  # Target Parquet file size in MB
    enable_explain_plans: bool = False  # Enable execution plan logging (DEBUG)

    # Validation thresholds
    min_bronze_records: int = 10_000_000  # Minimum 10M records expected
    max_null_rate: float = 0.05  # Maximum 5% null rate
    sao_paulo_expected_min: int = 1_000_000  # Minimum 1M records for SP
    sao_paulo_expected_max: int = 10_000_000  # Maximum 10M records for SP
    # Filial/Matriz ratio: most businesses are single-location (Matriz only),
    # so Filial count is typically much lower than Matriz count.
    # Real data shows ratio ~0.04 (4 branches per 100 headquarters).
    filial_matriz_ratio_min: float = 0.01  # Min Filial/Matriz ratio
    filial_matriz_ratio_max: float = 1.0  # Max Filial/Matriz ratio
    stability_threshold: float = 0.10  # 10% variance allowed vs previous

    def __post_init__(self):
        """Override from environment variables."""
        if env_bronze := os.getenv("BRONZE_LAYER_PATH"):
            self.bronze_layer_path = env_bronze
        if env_silver := os.getenv("SILVER_LAYER_PATH"):
            self.silver_layer_path = env_silver
        if env_gold := os.getenv("GOLD_LAYER_PATH"):
            self.gold_layer_path = env_gold

        # Create local directories only for filesystem paths (not S3A)
        for path in [self.bronze_layer_path, self.silver_layer_path, self.gold_layer_path]:
            if not path.startswith("s3a://"):
                Path(path).mkdir(parents=True, exist_ok=True)

    def get_bronze_path(self, execution_date: str) -> str:
        """Get partitioned Bronze layer path for execution date."""
        return f"{self.bronze_layer_path}/dt_ingestion={execution_date}"

    def get_silver_path(self, execution_date: str) -> str:
        """Get partitioned Silver layer path for execution date."""
        return f"{self.silver_layer_path}/dt_ingestion={execution_date}"

    def get_gold_path(self, execution_date: str) -> str:
        """Get partitioned Gold layer path for execution date."""
        return f"{self.gold_layer_path}/dt_ingestion={execution_date}"


@dataclass
class GlobalConfig:
    """Global configuration combining all config components."""

    download: DownloadConfig = field(default_factory=DownloadConfig)
    spark: SparkConfig = field(default_factory=SparkConfig)
    pipeline: PipelineConfig = field(default_factory=PipelineConfig)

    @classmethod
    def from_env(cls) -> "GlobalConfig":
        """Create configuration from environment variables."""
        return cls(
            download=DownloadConfig(),
            spark=SparkConfig(),
            pipeline=PipelineConfig()
        )


# Default global configuration instance
default_config = GlobalConfig.from_env()
