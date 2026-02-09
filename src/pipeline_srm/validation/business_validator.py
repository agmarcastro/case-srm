"""
Business Validator - Silver Layer Business Logic Checks.

Validates business rules and data consistency.
"""

import logging
from typing import Dict

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from pipeline_srm.config import PipelineConfig

logger = logging.getLogger(__name__)


class BusinessValidator:
    """
    Validates business logic for Silver layer.

    Checks:
    - Only São Paulo establishments
    - Valid establishment types (Matriz/Filial)
    - Valid registration statuses
    - CNPJ format (14 digits)
    - Record count within expected range
    - Filial/Matriz ratio validation
    """

    def __init__(self, config: PipelineConfig = None):
        """Initialize Business Validator."""
        self.config = config or PipelineConfig()

    def validate_silver_data(self, silver_df: DataFrame) -> Dict[str, bool]:
        """
        Validate Silver layer data against business rules.

        Args:
            silver_df: Silver DataFrame to validate

        Returns:
            dict: Validation results
        """
        results = {
            "only_sao_paulo": False,
            "valid_types": False,
            "valid_statuses": False,
            "valid_cnpj_format": False,
            "record_count_in_range": False,
            "ratio_in_range": False,
            "all_checks_passed": False
        }

        # Check only São Paulo
        non_sp_count = silver_df.filter(
            F.col("municipio") != self.config.SAO_PAULO_IBGE_CODE
        ).count()

        if non_sp_count == 0:
            results["only_sao_paulo"] = True
        else:
            logger.error(f"Found {non_sp_count} non-São Paulo records")

        # Check valid establishment types
        valid_types = {"Matriz", "Filial"}
        actual_types = set(
            row["establishment_type"]
            for row in silver_df.select("establishment_type").distinct().collect()
        )

        if actual_types.issubset(valid_types):
            results["valid_types"] = True
        else:
            invalid = actual_types - valid_types
            logger.error(f"Invalid establishment types: {invalid}")

        # Check valid registration statuses
        valid_statuses = {"NULA", "ATIVA", "SUSPENSA", "INAPTA", "BAIXADA"}
        actual_statuses = set(
            row["registration_status"]
            for row in silver_df.select("registration_status").distinct().collect()
        )

        if actual_statuses.issubset(valid_statuses):
            results["valid_statuses"] = True
        else:
            invalid = actual_statuses - valid_statuses
            logger.error(f"Invalid registration statuses: {invalid}")

        # Check CNPJ format (14 digits)
        invalid_cnpj_count = silver_df.filter(
            F.length(F.col("cnpj_completo")) != 14
        ).count()

        if invalid_cnpj_count == 0:
            results["valid_cnpj_format"] = True
        else:
            logger.warning(f"Found {invalid_cnpj_count} invalid CNPJ formats")

        # Check record count range
        record_count = silver_df.count()
        if (self.config.sao_paulo_expected_min <=
            record_count <=
            self.config.sao_paulo_expected_max):
            results["record_count_in_range"] = True
        else:
            logger.warning(
                f"Record count {record_count} outside expected range "
                f"[{self.config.sao_paulo_expected_min}, "
                f"{self.config.sao_paulo_expected_max}]"
            )

        # Check Filial/Matriz ratio
        type_counts = silver_df.groupBy("establishment_type").count().collect()
        type_dict = {row["establishment_type"]: row["count"] for row in type_counts}

        matriz_count = type_dict.get("Matriz", 0)
        filial_count = type_dict.get("Filial", 0)

        ratio = filial_count / matriz_count if matriz_count > 0 else 0

        if (self.config.filial_matriz_ratio_min <=
            ratio <=
            self.config.filial_matriz_ratio_max):
            results["ratio_in_range"] = True
        else:
            logger.warning(
                f"Filial/Matriz ratio {ratio:.2f} outside expected range "
                f"[{self.config.filial_matriz_ratio_min}, "
                f"{self.config.filial_matriz_ratio_max}]"
            )

        # All checks passed
        results["all_checks_passed"] = all([
            results["only_sao_paulo"],
            results["valid_types"],
            results["valid_statuses"],
            results["valid_cnpj_format"],
            results["record_count_in_range"],
            results["ratio_in_range"]
        ])

        logger.info(
            f"Business validation: {results['all_checks_passed']} "
            f"({record_count} records, ratio={ratio:.2f})"
        )

        return results
