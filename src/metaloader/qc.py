"""Quality Control module for metabolomics data."""

import logging
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Tuple, Any

from sqlalchemy import text
from sqlalchemy.orm import Session

logger = logging.getLogger(__name__)


@dataclass
class QCFilters:
    """Filters for QC queries."""
    study_id: Optional[str] = None
    analysis_id: Optional[str] = None


@dataclass
class QCResults:
    """Results from QC summary."""
    # Basic counts
    total_measurements: int = 0
    non_null_values: int = 0
    null_count: int = 0
    null_percent: float = 0.0

    # Duplicates
    duplicate_pairs_count: int = 0

    # Special values (PostgreSQL float8)
    nan_count: int = 0
    pos_inf_count: int = 0
    neg_inf_count: int = 0

    # Negative values
    negative_values_count: int = 0

    # Orphans
    orphan_sample_count: int = 0
    orphan_feature_count: int = 0

    # Top units
    top_units: List[Tuple[str, int]] = field(default_factory=list)

    # Top features with NULLs
    top_null_features: List[Tuple[str, int]] = field(default_factory=list)

    # Sample stats
    samples_total: int = 0
    samples_no_factors: int = 0

    # Filter info
    filters_applied: Dict[str, str] = field(default_factory=dict)


class QCService:
    """Service for running QC checks on metabolomics data."""

    def __init__(self, db: Session):
        self.db = db

    def _build_measurement_filter(
        self, filters: QCFilters
    ) -> Tuple[str, Dict[str, Any]]:
        """Build WHERE clause and params for measurement queries.

        Returns:
            Tuple of (where_clause, params_dict)
        """
        conditions = []
        params: Dict[str, Any] = {}

        if filters.study_id:
            # Filter by study: measurements -> samples -> study
            conditions.append("""
                m.sample_uid IN (
                    SELECT s.sample_uid FROM samples s
                    JOIN studies st ON s.study_pk = st.id
                    WHERE st.study_id = :study_id
                )
            """)
            params['study_id'] = filters.study_id

        if filters.analysis_id:
            # Filter by analysis: feature_uid has prefix <analysis_id>:
            conditions.append("m.feature_uid LIKE :analysis_prefix")
            params['analysis_prefix'] = f"{filters.analysis_id}:%"

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        return where_clause, params

    def _build_sample_filter(
        self, filters: QCFilters
    ) -> Tuple[str, Dict[str, Any]]:
        """Build WHERE clause and params for sample queries.

        Returns:
            Tuple of (where_clause, params_dict)
        """
        conditions = []
        params: Dict[str, Any] = {}

        if filters.study_id:
            conditions.append("""
                s.study_pk IN (
                    SELECT st.id FROM studies st
                    WHERE st.study_id = :study_id
                )
            """)
            params['study_id'] = filters.study_id

        where_clause = ""
        if conditions:
            where_clause = "WHERE " + " AND ".join(conditions)

        return where_clause, params

    def run_summary(self, filters: Optional[QCFilters] = None) -> QCResults:
        """Run full QC summary.

        Args:
            filters: Optional filters for study_id and/or analysis_id

        Returns:
            QCResults with all metrics
        """
        if filters is None:
            filters = QCFilters()

        results = QCResults()

        # Store applied filters
        if filters.study_id:
            results.filters_applied['study_id'] = filters.study_id
        if filters.analysis_id:
            results.filters_applied['analysis_id'] = filters.analysis_id

        # Build common filter
        where_clause, params = self._build_measurement_filter(filters)

        logger.debug(f"Running QC with filters: {filters}")

        # 1. Basic counts
        results.total_measurements, results.non_null_values = self._get_basic_counts(
            where_clause, params
        )
        results.null_count = results.total_measurements - results.non_null_values
        if results.total_measurements > 0:
            results.null_percent = (results.null_count / results.total_measurements) * 100

        # 2. Duplicate pairs
        results.duplicate_pairs_count = self._get_duplicate_count(where_clause, params)

        # 3. Special float values (NaN, Inf)
        results.nan_count, results.pos_inf_count, results.neg_inf_count = (
            self._get_special_values_count(where_clause, params)
        )

        # 4. Negative values
        results.negative_values_count = self._get_negative_count(where_clause, params)

        # 5. Orphan measurements
        results.orphan_sample_count, results.orphan_feature_count = (
            self._get_orphan_counts(where_clause, params)
        )

        # 6. Top units
        results.top_units = self._get_top_units(where_clause, params)

        # 7. Top features with NULLs
        results.top_null_features = self._get_top_null_features(where_clause, params)

        # 8. Sample stats (uses different filter)
        sample_where, sample_params = self._build_sample_filter(filters)
        results.samples_total, results.samples_no_factors = self._get_sample_stats(
            sample_where, sample_params
        )

        return results

    def _get_basic_counts(
        self, where_clause: str, params: Dict[str, Any]
    ) -> Tuple[int, int]:
        """Get total and non-null measurement counts."""
        query = text(f"""
            SELECT
                COUNT(*) as total,
                COUNT(m.value) as non_null
            FROM measurements m
            {where_clause}
        """)

        result = self.db.execute(query, params).fetchone()
        return result[0] or 0, result[1] or 0

    def _get_duplicate_count(
        self, where_clause: str, params: Dict[str, Any]
    ) -> int:
        """Get count of duplicate (sample_uid, feature_uid) pairs."""
        # First, we need to adapt the where clause for the subquery
        # We're looking for pairs that appear more than once
        query = text(f"""
            SELECT COUNT(*) FROM (
                SELECT m.sample_uid, m.feature_uid
                FROM measurements m
                {where_clause}
                GROUP BY m.sample_uid, m.feature_uid
                HAVING COUNT(*) > 1
            ) as duplicates
        """)

        result = self.db.execute(query, params).fetchone()
        return result[0] or 0

    def _get_special_values_count(
        self, where_clause: str, params: Dict[str, Any]
    ) -> Tuple[int, int, int]:
        """Get counts of NaN, +Inf, -Inf values.

        PostgreSQL float8 supports these special IEEE 754 values.
        """
        query = text(f"""
            SELECT
                COUNT(*) FILTER (WHERE m.value = 'NaN'::float8) as nan_count,
                COUNT(*) FILTER (WHERE m.value = 'Infinity'::float8) as pos_inf_count,
                COUNT(*) FILTER (WHERE m.value = '-Infinity'::float8) as neg_inf_count
            FROM measurements m
            {where_clause}
        """)

        result = self.db.execute(query, params).fetchone()
        return result[0] or 0, result[1] or 0, result[2] or 0

    def _get_negative_count(
        self, where_clause: str, params: Dict[str, Any]
    ) -> int:
        """Get count of negative values."""
        # Exclude special values
        extra_condition = "m.value < 0 AND m.value != '-Infinity'::float8"

        if where_clause:
            full_where = f"{where_clause} AND {extra_condition}"
        else:
            full_where = f"WHERE {extra_condition}"

        query = text(f"""
            SELECT COUNT(*)
            FROM measurements m
            {full_where}
        """)

        result = self.db.execute(query, params).fetchone()
        return result[0] or 0

    def _get_orphan_counts(
        self, where_clause: str, params: Dict[str, Any]
    ) -> Tuple[int, int]:
        """Get counts of orphan measurements (missing FK references)."""
        # Orphan samples (sample_uid not in samples table)
        query_sample = text(f"""
            SELECT COUNT(DISTINCT m.sample_uid)
            FROM measurements m
            LEFT JOIN samples s ON m.sample_uid = s.sample_uid
            {where_clause}
            {"AND" if where_clause else "WHERE"} s.sample_uid IS NULL
        """)

        # Orphan features (feature_uid not in features table)
        query_feature = text(f"""
            SELECT COUNT(DISTINCT m.feature_uid)
            FROM measurements m
            LEFT JOIN features f ON m.feature_uid = f.feature_uid
            {where_clause}
            {"AND" if where_clause else "WHERE"} f.feature_uid IS NULL
        """)

        orphan_samples = self.db.execute(query_sample, params).fetchone()[0] or 0
        orphan_features = self.db.execute(query_feature, params).fetchone()[0] or 0

        return orphan_samples, orphan_features

    def _get_top_units(
        self, where_clause: str, params: Dict[str, Any], limit: int = 10
    ) -> List[Tuple[str, int]]:
        """Get top N units by count."""
        query = text(f"""
            SELECT
                COALESCE(m.unit, '<NULL>') as unit_display,
                COUNT(*) as cnt
            FROM measurements m
            {where_clause}
            GROUP BY m.unit
            ORDER BY cnt DESC
            LIMIT :limit
        """)

        params_with_limit = {**params, 'limit': limit}
        result = self.db.execute(query, params_with_limit).fetchall()
        return [(row[0], row[1]) for row in result]

    def _get_top_null_features(
        self, where_clause: str, params: Dict[str, Any], limit: int = 10
    ) -> List[Tuple[str, int]]:
        """Get top N features with most NULL values."""
        # Add condition for NULL values
        if where_clause:
            full_where = f"{where_clause} AND m.value IS NULL"
        else:
            full_where = "WHERE m.value IS NULL"

        query = text(f"""
            SELECT
                m.feature_uid,
                COUNT(*) as null_count
            FROM measurements m
            {full_where}
            GROUP BY m.feature_uid
            ORDER BY null_count DESC
            LIMIT :limit
        """)

        params_with_limit = {**params, 'limit': limit}
        result = self.db.execute(query, params_with_limit).fetchall()
        return [(row[0], row[1]) for row in result]

    def _get_sample_stats(
        self, where_clause: str, params: Dict[str, Any]
    ) -> Tuple[int, int]:
        """Get sample statistics."""
        query = text(f"""
            SELECT
                COUNT(*) as total,
                COUNT(*) FILTER (WHERE s.factors_raw IS NULL) as no_factors
            FROM samples s
            {where_clause}
        """)

        result = self.db.execute(query, params).fetchone()
        return result[0] or 0, result[1] or 0
