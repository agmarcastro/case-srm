"""
Input Validator - Bronze Layer Data Quality Checks.

Validates raw data integrity before processing.
"""

import logging
import zipfile
from pathlib import Path
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline_srm.config import PipelineConfig

logger = logging.getLogger(__name__)


class InputValidator:
    """
    Validates input data quality for Bronze layer.

    Checks:
    - ZIP file integrity
    - CSV schema completeness
    - Minimum record count
    - Null rate thresholds
    """

    def __init__(self, config: PipelineConfig = None):
        """Initialize Input Validator."""
        self.config = config or PipelineConfig()

    def validate_zip_file(self, zip_path: Path) -> Dict[str, bool]:
        """
        Validate ZIP file integrity.

        Args:
            zip_path: Path to ZIP file

        Returns:
            dict: Validation results
        """
        results = {
            "file_exists": False,
            "is_valid_zip": False,
            "has_content": False,
            "all_checks_passed": False
        }

        # Check file exists
        if not zip_path.exists():
            logger.error(f"ZIP file does not exist: {zip_path}")
            return results

        results["file_exists"] = True

        # Check ZIP validity
        try:
            with zipfile.ZipFile(zip_path, 'r') as zip_ref:
                bad_file = zip_ref.testzip()
                if bad_file is not None:
                    logger.error(f"Corrupt file in ZIP: {bad_file}")
                    return results

                results["is_valid_zip"] = True

                # Check has content
                if len(zip_ref.namelist()) > 0:
                    results["has_content"] = True

        except zipfile.BadZipFile as e:
            logger.error(f"Invalid ZIP file {zip_path}: {e}")
            return results

        # All checks passed
        results["all_checks_passed"] = all([
            results["file_exists"],
            results["is_valid_zip"],
            results["has_content"]
        ])

        logger.info(f"ZIP validation: {results['all_checks_passed']}")

        return results

    def validate_csv_schema(self, df: DataFrame) -> Dict[str, bool]:
        """
        Validate CSV DataFrame schema and data quality.

        Args:
            df: Bronze DataFrame to validate

        Returns:
            dict: Validation results
        """
        results = {
            "has_minimum_records": False,
            "has_required_fields": False,
            "null_rate_acceptable": False,
            "all_checks_passed": False
        }

        # Check minimum record count
        record_count = df.count()
        if record_count >= self.config.min_bronze_records:
            results["has_minimum_records"] = True
        else:
            logger.warning(
                f"Record count {record_count} below minimum "
                f"{self.config.min_bronze_records}"
            )

        # Check required fields present
        required_fields = ["cnpj_basico", "cnpj_ordem", "cnpj_dv",
                          "municipio", "situacao_cadastral"]
        actual_fields = set(df.columns)

        if all(field in actual_fields for field in required_fields):
            results["has_required_fields"] = True
        else:
            missing = set(required_fields) - actual_fields
            logger.error(f"Missing required fields: {missing}")

        # Check null rates
        null_rates = self._calculate_null_rates(df)
        max_null_rate = max(null_rates.values()) if null_rates else 0

        if max_null_rate <= self.config.max_null_rate:
            results["null_rate_acceptable"] = True
        else:
            logger.warning(
                f"High null rate detected: {max_null_rate:.2%} "
                f"(threshold: {self.config.max_null_rate:.2%})"
            )

        # All checks passed
        results["all_checks_passed"] = all([
            results["has_minimum_records"],
            results["has_required_fields"],
            results["null_rate_acceptable"]
        ])

        logger.info(
            f"CSV schema validation: {results['all_checks_passed']} "
            f"({record_count} records)"
        )

        return results

    def _calculate_null_rates(self, df: DataFrame) -> Dict[str, float]:
        """Calculate null rates for all columns."""
        total_count = df.count()
        null_rates = {}

        for col_name in df.columns:
            null_count = df.filter(F.col(col_name).isNull()).count()
            null_rates[col_name] = null_count / total_count if total_count > 0 else 0

        return null_rates
