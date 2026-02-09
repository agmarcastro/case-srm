"""
DAG: cnpj_extraction_pipeline
Description: Extract and aggregate CNPJ data for São Paulo establishments
Schedule: Daily at 2 AM (0 2 * * *)
Owner: Agmar Castro - SRM

Pipeline Flow:
  Download → Bronze → Silver → Gold → Validate → Report

Data Sources (Federal Revenue):
  - Estabelecimentos0-9.zip: Establishment records (~600MB each, 10 files)
  - Municipios.zip: City codes and names (for São Paulo filtering)

Architecture:
  - Bronze: Raw CSV → Parquet (preserve source fidelity)
  - Silver: Filter São Paulo + standardize fields (63% I/O reduction via column pruning)
  - Gold: Aggregate by type × status (business metrics)
  - Validation: Business rules + data quality checks
  - Report: Summary generation with Filial/Matriz ratios

Performance Optimizations:
  - Parallel file downloads via ThreadPoolExecutor (2x speedup, memory-safe)
  - TaskFlow API for automatic XCom handling
  - Column pruning: 11 of 30 Bronze columns
  - Adaptive Query Execution (AQE) enabled
  - Partition optimization: 256MB target file size
  - Exponential backoff retry strategy

Dependencies:
  - pipeline_srm package (optimized with Spark performance patterns)
  - PySpark 3.5+ with AQE support

Required Airflow Connections:
  - spark_default: Local Spark cluster or YARN/K8s
  - slack_alerts (optional): Failure notifications

Required Airflow Variables:
  - cnpj_base_url: Federal Revenue download URL
  - bronze_layer_path: Bronze storage location (/tmp/cnpj/bronze)
  - silver_layer_path: Silver storage location (/tmp/cnpj/silver)
  - gold_layer_path: Gold storage location (/tmp/cnpj/gold)

"""

from datetime import datetime, timedelta
from typing import Dict, List

from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.models.param import Param
from airflow.utils.task_group import TaskGroup


def notify_failure(context):
    """
    Send alert on task failure.

    Supports Slack webhook or email notifications.
    Configure via Airflow connection: slack_alerts
    """
    try:
        from airflow.providers.slack.hooks.slack_webhook import SlackWebhookHook

        task_instance = context['task_instance']
        dag_id = task_instance.dag_id
        task_id = task_instance.task_id
        execution_date = context['execution_date']
        log_url = task_instance.log_url

        slack_webhook = SlackWebhookHook(slack_webhook_conn_id='slack_alerts')
        slack_webhook.send_text(
            text=f"❌ *Pipeline Failure*\n"
                 f"DAG: `{dag_id}`\n"
                 f"Task: `{task_id}`\n"
                 f"Execution Date: {execution_date}\n"
                 f"<{log_url}|View Logs>"
        )
    except Exception as e:
        # Fallback to logging if Slack unavailable
        print(f"Failed to send Slack notification: {e}")
        print(f"Task {context['task_instance'].task_id} failed at {context['execution_date']}")


def get_reference_month_from_context() -> str:
    """
    Get reference month from DAG params or Airflow context.

    CNPJ data from Federal Revenue is updated monthly.
    This function returns the reference month in YYYY-MM format.

    Priority:
    1. params['reference_month'] (manual trigger override)
    2. Current month derived from context['ds'] (Airflow logical execution date)

    This enables CLI-style parameterization:
        airflow dags trigger dag_id --conf '{"reference_month": "2026-02"}'

    Returns:
        Reference month in YYYY-MM format
    """
    from airflow.operators.python import get_current_context

    context = get_current_context()
    params = context.get('params', {})

    # Check if reference_month was provided via params (manual trigger)
    param_month = params.get('reference_month')
    if param_month:
        print(f"📅 Using parameterized reference_month: {param_month}")
        return param_month

    # Fall back to Airflow's logical execution date (extract YYYY-MM)
    logical_date = context['ds']  # Format: YYYY-MM-DD
    reference_month = logical_date[:7]  # Extract YYYY-MM
    print(f"📅 Using Airflow logical month: {reference_month}")
    return reference_month


@dag(
    dag_id='cnpj_extraction_pipeline_v2',
    schedule='0 2 * * *',  # Daily at 2 AM
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'etl', 'sao-paulo', 'production', 'medallion'],
    default_args={
        'owner': 'Agmar Castro - SRM',
        'depends_on_past': False,
        'email_on_failure': False,
        'email_on_retry': False,
        'retries': 3,
        'retry_delay': timedelta(minutes=5),
        'retry_exponential_backoff': True,  # ✅ Exponential backoff
        'max_retry_delay': timedelta(minutes=30),  # ✅ Cap max delay
        'execution_timeout': timedelta(hours=2),
        'on_failure_callback': notify_failure,  # ✅ Failure alerting
    },
    doc_md=__doc__,
    # ========================================
    # DAG PARAMETERS (for manual triggers)
    # ========================================
    # These params enable CLI-style flexibility via Airflow UI/API
    # Example trigger: airflow dags trigger cnpj_extraction_pipeline_v2 --conf '{"reference_month": "2026-02", "job_type": "full"}'
    params={
        'reference_month': Param(
            default=datetime.now().strftime('%Y-%m'),
            type='string',
            description='Reference month for CNPJ data (YYYY-MM). Defaults to current month.',
            pattern=r'^\d{4}-(0[1-9]|1[0-2])$',
        ),
        'job_type': Param(
            default='full',
            type='string',
            enum=['full', 'bronze_only', 'silver_only', 'gold_only', 'validate_only'],
            description='Pipeline stage to execute. "full" runs all stages.',
        ),
        'skip_download': Param(
            default=False,
            type='boolean',
            description='Skip download step (use existing Bronze data)',
        ),
        'force_reprocess': Param(
            default=False,
            type='boolean',
            description='Force reprocessing even if data exists for the month',
        ),
    },
)
def cnpj_extraction_pipeline():
    """
    CNPJ Extraction Pipeline - TaskFlow API Implementation.

    Orchestrates the full Medallion architecture pipeline:
    Bronze (Raw) → Silver (Refined) → Gold (Business)

    Features:
    - Idempotent operations (safe to retry)
    - Automatic XCom serialization with type hints
    - Column pruning for 63% I/O reduction
    - Adaptive Query Execution (AQE) enabled
    - Comprehensive data quality validation
    """

    # ========================================
    # EXTRACTION GROUP
    # ========================================

    @task()
    def download_files() -> List[str]:
        """
        Download CNPJ files from Federal Revenue with parallel downloads.

        Downloads both:
        - Estabelecimentos0.zip through Estabelecimentos9.zip (establishments)
        - Municipios.zip (city codes for filtering by city name)

        Performance: Uses ThreadPoolExecutor with 2 workers (~2x speedup).
        Memory: Limited to 2 concurrent downloads to avoid OOM in containers.
        Idempotency: Downloads to timestamped directory. Safe to retry.

        Returns:
            List of CSV file paths
        """
        from pipeline_srm.config import DownloadConfig
        from pipeline_srm.ingestion.downloader import CNPJDownloader

        # Get reference month (supports param override for manual triggers)
        reference_month = get_reference_month_from_context()

        config = DownloadConfig()
        downloader = CNPJDownloader(config)

        # Download all enabled file types with parallel execution
        # Using max_workers=2 to avoid OOM in memory-constrained containers
        # Each file is ~600MB, so 2 concurrent downloads = ~1.2GB peak memory
        result = downloader.download_all_parallel(reference_month, max_workers=2)

        # Log download summary by file type
        for file_type, files in result.items():
            print(f"  {file_type.value}: {len(files)} CSV files")

        # Flatten to list of paths for downstream tasks
        all_files = [str(f) for paths in result.values() for f in paths]
        print(f"✅ Downloaded {len(all_files)} total CSV files for {reference_month}")

        return all_files

    @task()
    def create_spark_config() -> Dict[str, str]:
        """
        Create Spark configuration dictionary.

        This eliminates redundant Spark session creation by
        generating config once and passing to all tasks.

        Configuration includes:
        - Spark master URL (cluster or local mode)
        - AQE enabled (runtime query optimization)
        - Dynamic shuffle partitions (56 for 14 CPU cores)
        - Memory tuning (80% execution + storage)
        - Event logging for performance monitoring
        - S3/MinIO configuration (if S3_ENDPOINT is set)

        Returns:
            Spark configuration dictionary
        """
        import os
        from pipeline_srm.config import GlobalConfig

        config = GlobalConfig.from_env()
        spark_config = config.spark.get_spark_config_dict()

        # Add Spark master URL from environment (for cluster mode)
        spark_master = os.getenv('SPARK_MASTER_URL', 'local[*]')
        spark_config['spark.master'] = spark_master

        print(f"📊 Spark Configuration:")
        print(f"  - Master: {spark_master}")
        print(f"  - AQE Enabled: {spark_config.get('spark.sql.adaptive.enabled')}")
        print(f"  - Shuffle Partitions: {spark_config.get('spark.sql.shuffle.partitions')}")
        print(f"  - Memory Fraction: {spark_config.get('spark.memory.fraction')}")
        if spark_config.get('spark.hadoop.fs.s3a.endpoint'):
            print(f"  - S3 Endpoint: {spark_config.get('spark.hadoop.fs.s3a.endpoint')}")

        return spark_config

    @task()
    def load_bronze(csv_files: List[str], spark_config: Dict[str, str]) -> int:
        """
        Load CSV files to Bronze layer as Parquet.

        Idempotency: Uses overwrite mode for execution date partition.
        Safe to retry without creating duplicates.

        Performance:
        - Explicit schema (no inference)
        - Corrupt record validation (<1% threshold)
        - Partition optimization (256MB target file size)

        Args:
            csv_files: List of CSV file paths from download task
            spark_config: Spark configuration dictionary

        Returns:
            Number of records loaded
        """
        from pathlib import Path
        from pyspark.sql import SparkSession
        from pipeline_srm.config import GlobalConfig
        from pipeline_srm.bronze.bronze_loader import BronzeLoader

        # Get reference month (supports param override for manual triggers)
        reference_month = get_reference_month_from_context()

        # Convert strings back to Paths
        csv_paths = [Path(f) for f in csv_files]

        # Create Spark session with optimized config (connects to cluster if SPARK_MASTER_URL set)
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()

        try:
            config = GlobalConfig.from_env()
            loader = BronzeLoader(spark, config.pipeline)

            # Load to Bronze with validation
            loader.load_csv_to_bronze(csv_paths, reference_month)

            # Read count from written Parquet (cheap metadata scan)
            # instead of counting the CSV-backed DataFrame (expensive full scan)
            bronze_path = config.pipeline.get_bronze_path(reference_month)
            record_count = spark.read.parquet(str(bronze_path)).count()

            print(f"✅ Bronze: Loaded {record_count:,} records")
            return record_count

        finally:
            spark.stop()

    # ========================================
    # TRANSFORMATION GROUP
    # ========================================

    @task()
    def transform_silver(spark_config: Dict[str, str]) -> int:
        """
        Transform Bronze to Silver layer.

        Idempotency: Uses overwrite mode for execution date partition.
        Safe to retry without creating duplicates.

        Performance:
        - Column pruning: 11 of 30 columns (63% I/O reduction)
        - Predicate pushdown: São Paulo filter pushed to Parquet reader
        - No premature count() calls (Spark optimizes full DAG)

        Transformations:
        - Filter: São Paulo establishments (IBGE 3550308)
        - Standardize: Status codes → labels, Type codes → labels
        - Create: CNPJ completo (14 digits)

        Args:
            spark_config: Spark configuration dictionary

        Returns:
            Number of São Paulo records
        """
        from pyspark.sql import SparkSession
        from pipeline_srm.config import GlobalConfig
        from pipeline_srm.silver.silver_transformer import SilverTransformer

        # Get reference month (supports param override for manual triggers)
        reference_month = get_reference_month_from_context()

        # Create Spark session (connects to cluster if SPARK_MASTER_URL set)
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()

        try:
            config = GlobalConfig.from_env()
            transformer = SilverTransformer(spark, config.pipeline)

            # Read Bronze with column pruning (performance optimization)
            bronze_df = transformer.read_bronze_with_pruning(reference_month)

            # Transform to Silver
            silver_df = transformer.transform_to_silver(bronze_df, reference_month)
            record_count = silver_df.count()

            print(f"✅ Silver: Transformed {record_count:,} São Paulo records")
            return record_count

        finally:
            spark.stop()

    @task()
    def aggregate_gold(spark_config: Dict[str, str]) -> int:
        """
        Aggregate Silver to Gold layer.

        Idempotency: Uses overwrite mode for execution date partition.
        Safe to retry without creating duplicates.

        Performance:
        - Cache Silver DataFrame (used for count + aggregation)
        - Unpersist after aggregation completes
        - Coalesce to 1 partition (Gold data is small)

        Aggregation:
        - Group by: establishment_type × registration_status
        - Validates: Sum of aggregation == Silver count

        Args:
            spark_config: Spark configuration dictionary

        Returns:
            Number of aggregated groups
        """
        from pyspark.sql import SparkSession
        from pipeline_srm.config import GlobalConfig
        from pipeline_srm.silver.silver_transformer import SilverTransformer
        from pipeline_srm.gold.gold_aggregator import GoldAggregator

        # Get reference month (supports param override for manual triggers)
        reference_month = get_reference_month_from_context()

        # Create Spark session (connects to cluster if SPARK_MASTER_URL set)
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()

        try:
            config = GlobalConfig.from_env()

            # Read Silver layer
            transformer = SilverTransformer(spark, config.pipeline)
            silver_df = transformer.read_silver_layer(reference_month)

            # Aggregate to Gold with validation
            aggregator = GoldAggregator(spark, config.pipeline)
            gold_df = aggregator.aggregate_establishment_counts(silver_df, reference_month)

            group_count = gold_df.count()
            print(f"✅ Gold: Aggregated {group_count} groups")
            return group_count

        finally:
            spark.stop()

    # ========================================
    # VALIDATION GROUP
    # ========================================

    @task()
    def validate_output(spark_config: Dict[str, str]) -> Dict[str, bool]:
        """
        Run comprehensive validation checks.

        Business Validation:
        - Only São Paulo establishments
        - Valid establishment types (Matriz/Filial)
        - Valid registration statuses
        - CNPJ format (14 digits)
        - Record count within expected range
        - Filial/Matriz ratio validation

        Output Validation:
        - Aggregation sum matches Silver count
        - No null keys in aggregation
        - Active records exist
        - Stability vs previous execution

        Args:
            spark_config: Spark configuration dictionary

        Returns:
            Validation results dictionary

        Raises:
            ValueError: If any validation fails
        """
        from pyspark.sql import SparkSession
        from pipeline_srm.config import GlobalConfig
        from pipeline_srm.silver.silver_transformer import SilverTransformer
        from pipeline_srm.gold.gold_aggregator import GoldAggregator
        from pipeline_srm.validation.business_validator import BusinessValidator
        from pipeline_srm.validation.output_validator import OutputValidator

        # Get reference month (supports param override for manual triggers)
        reference_month = get_reference_month_from_context()

        # Create Spark session (connects to cluster if SPARK_MASTER_URL set)
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()

        try:
            config = GlobalConfig.from_env()

            # Read Silver and Gold layers
            transformer = SilverTransformer(spark, config.pipeline)
            silver_df = transformer.read_silver_layer(reference_month)

            aggregator = GoldAggregator(spark, config.pipeline)
            gold_df = aggregator.read_gold_layer(reference_month)

            # Validate Silver layer (business rules)
            business_validator = BusinessValidator(config.pipeline)
            business_results = business_validator.validate_silver_data(silver_df)

            # Validate Gold layer (output quality)
            output_validator = OutputValidator(config.pipeline)
            output_results = output_validator.validate_gold_aggregation(
                silver_df,
                gold_df
            )

            # Check if all validations passed
            all_passed = (
                business_results['all_checks_passed'] and
                output_results['all_checks_passed']
            )

            if not all_passed:
                print("❌ Validation Failed:")
                print(f"  Business: {business_results}")
                print(f"  Output: {output_results}")
                raise ValueError(
                    f"Validation failed - Business: {business_results['all_checks_passed']}, "
                    f"Output: {output_results['all_checks_passed']}"
                )

            print("✅ All validations passed")

            return {
                'business_passed': business_results['all_checks_passed'],
                'output_passed': output_results['all_checks_passed'],
                'all_passed': all_passed,
            }

        finally:
            spark.stop()

    @task()
    def generate_report(spark_config: Dict[str, str]) -> Dict[str, any]:
        """
        Generate summary report from Gold layer.

        Report includes:
        - Matriz active count
        - Filial active count
        - Total active establishments
        - Filial/Matriz ratio
        - Status distribution

        Safety:
        - Validates row count < 10K before toPandas()
        - Prevents OOM on unexpected aggregation size

        Args:
            spark_config: Spark configuration dictionary

        Returns:
            Summary report dictionary
        """
        from pyspark.sql import SparkSession
        from pipeline_srm.config import GlobalConfig
        from pipeline_srm.gold.gold_aggregator import GoldAggregator

        # Get reference month (supports param override for manual triggers)
        reference_month = get_reference_month_from_context()

        # Create Spark session (connects to cluster if SPARK_MASTER_URL set)
        builder = SparkSession.builder
        for key, value in spark_config.items():
            builder = builder.config(key, value)
        spark = builder.getOrCreate()

        try:
            config = GlobalConfig.from_env()
            aggregator = GoldAggregator(spark, config.pipeline)

            # Read Gold layer
            gold_df = aggregator.read_gold_layer(reference_month)

            # Generate summary (includes toPandas() safety check)
            summary = aggregator.generate_summary_report(gold_df)

            # Pretty print summary
            print("=" * 70)
            print("📊 CNPJ PIPELINE EXECUTION SUMMARY")
            print("=" * 70)
            print(f"Reference Month:     {reference_month}")
            print(f"Matriz Active:       {summary['matriz_active']:,}")
            print(f"Filial Active:       {summary['filial_active']:,}")
            print(f"Total Active:        {summary['total_active']:,}")
            print(f"Filial/Matriz Ratio: {summary['ratio_filial_matriz']:.2f}")
            print("=" * 70)

            return summary

        finally:
            spark.stop()

    # ========================================
    # TASK ORCHESTRATION
    # ========================================

    # Shared Spark configuration
    spark_config = create_spark_config()

    # Extraction group (parallel downloads via ThreadPoolExecutor inside task)
    with TaskGroup("extraction", tooltip="Download and load to Bronze") as extraction:
        files = download_files()
        bronze_count = load_bronze(files, spark_config)

    # Transformation group
    with TaskGroup("transformation", tooltip="Transform to Silver and Gold") as transformation:
        silver_count = transform_silver(spark_config)
        gold_count = aggregate_gold(spark_config)
        silver_count >> gold_count

    # Validation group
    with TaskGroup("validation", tooltip="Validate and report") as validation_group:
        validation_results = validate_output(spark_config)
        report = generate_report(spark_config)
        validation_results >> report

    # Define dependencies between groups
    extraction >> transformation >> validation_group


# Instantiate the DAG
cnpj_pipeline = cnpj_extraction_pipeline()


# ============================================
# CLI WRAPPER DAG (Alternative Approach)
# ============================================
# This DAG provides a simpler CLI-based execution style
# Equivalent to: docker exec srm-spark-master python -m pipeline_srm.cli extract --execution-date 2026-02-05

@dag(
    dag_id='cnpj_cli_runner',
    schedule=None,  # Manual trigger only
    start_date=datetime(2026, 2, 1),
    catchup=False,
    max_active_runs=1,
    tags=['cnpj', 'cli', 'manual', 'utility'],
    default_args={
        'owner': 'pipeline-srm',
        'retries': 1,
        'retry_delay': timedelta(minutes=2),
        'execution_timeout': timedelta(hours=2),
    },
    params={
        'reference_month': Param(
            default=datetime.now().strftime('%Y-%m'),
            type='string',
            description='Reference month for CNPJ data (YYYY-MM). Defaults to current month.',
            pattern=r'^\d{4}-(0[1-9]|1[0-2])$',
        ),
        'job_type': Param(
            default='extract',
            type='string',
            enum=['extract', 'transform', 'aggregate', 'validate', 'full'],
            description='Job type to execute via CLI',
        ),
        'spark_master': Param(
            default='spark://spark-master:7077',
            type='string',
            description='Spark master URL',
        ),
    },
    doc_md="""
    ## CNPJ CLI Runner

    **Purpose**: Execute pipeline_srm CLI commands within Airflow orchestration.

    This DAG wraps the CLI for scenarios where:
    - You need quick ad-hoc runs without full DAG complexity
    - You want to trigger specific stages independently
    - You're testing/debugging individual pipeline components

    ### Trigger Examples

    **Via Airflow UI:**
    1. Go to DAGs → cnpj_cli_runner → Trigger DAG
    2. Fill in parameters (reference_month, job_type)
    3. Click Trigger

    **Via CLI:**
    ```bash
    airflow dags trigger cnpj_cli_runner --conf '{"reference_month": "2026-02", "job_type": "extract"}'
    ```

    **Via REST API:**
    ```bash
    curl -X POST 'http://localhost:8081/api/v1/dags/cnpj_cli_runner/dagRuns' \\
      -H 'Content-Type: application/json' \\
      -u admin:DevPassword123 \\
      -d '{"conf": {"reference_month": "2026-02", "job_type": "full"}}'
    ```
    """,
)
def cnpj_cli_runner():
    """
    CLI wrapper DAG for parameterized pipeline execution.
    """
    from airflow.providers.docker.operators.docker import DockerOperator
    from airflow.operators.bash import BashOperator

    @task()
    def build_cli_command() -> str:
        """
        Build the CLI command based on parameters.

        Returns:
            CLI command string to execute
        """
        from airflow.operators.python import get_current_context

        context = get_current_context()
        params = context['params']

        reference_month = params['reference_month']
        job_type = params['job_type']

        # Build command based on job_type
        base_cmd = f"python -m pipeline_srm.cli {job_type} --reference-month {reference_month}"

        print(f"🔧 Built CLI command: {base_cmd}")
        return base_cmd

    @task()
    def execute_spark_job(cli_command: str) -> Dict[str, any]:
        """
        Execute the Spark job via subprocess.

        This runs directly on the Airflow worker, which has access
        to the Spark cluster via spark-submit or local Spark.

        Args:
            cli_command: The CLI command to execute

        Returns:
            Execution result with status and output
        """
        import subprocess
        import os
        from airflow.operators.python import get_current_context

        context = get_current_context()
        params = context['params']

        # Set Spark master from params
        spark_master = params.get('spark_master', 'spark://spark-master:7077')
        os.environ['SPARK_MASTER_URL'] = spark_master

        print(f"🚀 Executing: {cli_command}")
        print(f"📡 Spark Master: {spark_master}")

        try:
            result = subprocess.run(
                cli_command,
                shell=True,
                capture_output=True,
                text=True,
                timeout=7200,  # 2 hour timeout
                env={**os.environ, 'SPARK_MASTER_URL': spark_master}
            )

            if result.returncode != 0:
                print(f"❌ STDERR: {result.stderr}")
                raise RuntimeError(f"CLI command failed with exit code {result.returncode}")

            print(f"✅ STDOUT: {result.stdout}")

            return {
                'status': 'success',
                'exit_code': result.returncode,
                'stdout': result.stdout[-5000:] if len(result.stdout) > 5000 else result.stdout,
            }

        except subprocess.TimeoutExpired:
            raise RuntimeError("CLI command timed out after 2 hours")

    # Task flow
    cli_cmd = build_cli_command()
    result = execute_spark_job(cli_cmd)


# Instantiate CLI runner DAG
cli_runner = cnpj_cli_runner()
