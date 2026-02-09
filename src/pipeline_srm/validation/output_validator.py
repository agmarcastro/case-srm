"""
Output Validator - Gold Layer Output Quality Checks.

Validates final aggregations and consistency.
"""

import logging
from typing import Dict, Optional

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline_srm.config import PipelineConfig

logger = logging.getLogger(__name__)


class OutputValidator:
    """
    Validates output data quality for Gold layer.

    Checks:
    - Aggregation sum matches Silver count
    - No null keys in aggregation
    - Active count > 0
    - Stability check vs previous execution
    """

    def __init__(self, config: PipelineConfig = None):
        """Initialize Output Validator."""
        self.config = config or PipelineConfig()

    def validate_gold_aggregation(
        self,
        silver_df: DataFrame,
        gold_df: DataFrame,
        previous_gold_df: Optional[DataFrame] = None
    ) -> Dict[str, bool]:
        """
        Validate Gold layer aggregation against Silver source.

        Args:
            silver_df: Source Silver DataFrame
            gold_df: Aggregated Gold DataFrame
            previous_gold_df: Previous Gold DataFrame for stability check

        Returns:
            dict: Validation results
        """
        results = {
            "sum_matches_silver": False,
            "no_null_keys": False,
            "has_active_records": False,
            "stable_vs_previous": True,  # Default True if no previous
            "all_checks_passed": False
        }

        # Check aggregation sum matches Silver count
        silver_count = silver_df.count()
        gold_sum = gold_df.select(F.sum("count").alias("total")).collect()[0]["total"]

        if gold_sum == silver_count:
            results["sum_matches_silver"] = True
        else:
            logger.error(
                f"Aggregation sum mismatch: Gold={gold_sum}, Silver={silver_count}"
            )

        # Check no null keys
        null_type_count = gold_df.filter(
            F.col("establishment_type").isNull()
        ).count()
        null_status_count = gold_df.filter(
            F.col("registration_status").isNull()
        ).count()

        if null_type_count == 0 and null_status_count == 0:
            results["no_null_keys"] = True
        else:
            logger.error(
                f"Null keys found: type={null_type_count}, status={null_status_count}"
            )

        # Check has active records
        active_count = gold_df.filter(
            F.col("registration_status") == "ATIVA"
        ).select(F.sum("count").alias("total")).collect()[0]["total"]

        if active_count and active_count > 0:
            results["has_active_records"] = True
        else:
            logger.error("No active records found in Gold layer")

        # Check stability vs previous (if provided)
        if previous_gold_df is not None:
            try:
                current_active = active_count or 0
                previous_active = previous_gold_df.filter(
                    F.col("registration_status") == "ATIVA"
                ).select(F.sum("count").alias("total")).collect()[0]["total"] or 0

                variance = abs(
                    (current_active - previous_active) / previous_active
                ) if previous_active > 0 else 0

                if variance <= self.config.stability_threshold:
                    results["stable_vs_previous"] = True
                else:
                    logger.warning(
                        f"Unstable vs previous: variance={variance:.2%} "
                        f"(threshold={self.config.stability_threshold:.2%})"
                    )
                    results["stable_vs_previous"] = False

            except Exception as e:
                logger.warning(f"Could not validate stability: {e}")
                results["stable_vs_previous"] = True  # Don't fail on this

        # All checks passed
        results["all_checks_passed"] = all([
            results["sum_matches_silver"],
            results["no_null_keys"],
            results["has_active_records"],
            results["stable_vs_previous"]
        ])

        logger.info(
            f"Output validation: {results['all_checks_passed']} "
            f"(active={active_count})"
        )

        return results
