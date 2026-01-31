"""Service for exporting data to Parquet format."""

import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Iterator, Optional
from uuid import UUID

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from sqlalchemy import text
from sqlalchemy.engine import Engine

logger = logging.getLogger(__name__)

# Default chunk size for streaming export
DEFAULT_CHUNK_SIZE = 200_000


@dataclass
class ExportStats:
    """Statistics from export operation."""
    output_path: str
    total_rows: int = 0
    total_chunks: int = 0
    file_size_bytes: int = 0


class ExportService:
    """Service for exporting measurement data to Parquet."""

    # SQL query for long-format export with all joins
    EXPORT_QUERY = """
        SELECT
            -- File info
            f.id::text AS file_id,
            f.path_rel,
            f.detected_type,
            f.device,
            f.exposure,
            f.sample_type,
            f.platform,

            -- Sample info
            s.sample_uid,
            s.sample_label,

            -- Feature info
            ft.feature_uid,
            ft.feature_type,
            ft.name_raw AS feature_name,
            ft.refmet_name,

            -- Measurement data
            m.value,
            m.unit,
            m.col_index,
            m.replicate_ix,

            -- Study/Analysis info (if available)
            st.study_id,
            a.analysis_id,

            -- Timestamps
            m.created_at

        FROM measurements m
        LEFT JOIN files f ON m.file_id = f.id
        LEFT JOIN samples s ON m.sample_uid = s.sample_uid
        LEFT JOIN features ft ON m.feature_uid = ft.feature_uid
        LEFT JOIN studies st ON s.study_pk = st.id
        LEFT JOIN analyses a ON ft.analysis_id = a.analysis_id AND a.study_pk = st.id
        WHERE 1=1
        {filters}
        ORDER BY f.id, m.col_index, ft.feature_uid
    """

    def __init__(self, engine: Engine):
        self.engine = engine

    def export_parquet(
        self,
        output_path: Path,
        file_id: Optional[UUID] = None,
        import_id: Optional[UUID] = None,
        feature_type: Optional[str] = None,
        study_id: Optional[str] = None,
        chunk_size: int = DEFAULT_CHUNK_SIZE,
    ) -> ExportStats:
        """Export measurement data to Parquet file.

        Streams data in chunks to avoid loading entire dataset into memory.
        Uses zstd compression for efficient storage.

        Args:
            output_path: Path for output Parquet file
            file_id: Filter by specific file
            import_id: Filter by import
            feature_type: Filter by feature type (e.g., 'metabolite', 'nmr_bin')
            study_id: Filter by study ID
            chunk_size: Number of rows per chunk

        Returns:
            ExportStats with export statistics
        """
        stats = ExportStats(output_path=str(output_path))

        # Build filter clause
        filters = self._build_filters(file_id, import_id, feature_type, study_id)
        query = self.EXPORT_QUERY.format(filters=filters)

        logger.info(f"Starting export to {output_path}")
        logger.info(f"Chunk size: {chunk_size:,}")

        # Define a consistent schema for all chunks
        # This ensures schema consistency even when some chunks have all NULLs
        export_schema = pa.schema([
            ('file_id', pa.large_string()),
            ('path_rel', pa.large_string()),
            ('detected_type', pa.large_string()),
            ('device', pa.large_string()),
            ('exposure', pa.large_string()),
            ('sample_type', pa.large_string()),
            ('platform', pa.large_string()),
            ('sample_uid', pa.large_string()),
            ('sample_label', pa.large_string()),
            ('feature_uid', pa.large_string()),
            ('feature_type', pa.large_string()),
            ('feature_name', pa.large_string()),
            ('refmet_name', pa.large_string()),
            ('value', pa.float64()),
            ('unit', pa.large_string()),
            ('col_index', pa.float64()),
            ('replicate_ix', pa.float64()),
            ('study_id', pa.large_string()),
            ('analysis_id', pa.large_string()),
            ('created_at', pa.large_string()),
        ])

        # Stream chunks and write to Parquet
        writer = None

        try:
            for chunk_df in self._stream_chunks(query, chunk_size):
                chunk_num = stats.total_chunks + 1
                rows_in_chunk = len(chunk_df)

                logger.info(f"Processing chunk {chunk_num}: {rows_in_chunk:,} rows")

                # Convert to PyArrow table with consistent schema
                table = pa.Table.from_pandas(
                    chunk_df, 
                    schema=export_schema,
                    preserve_index=False
                )

                # Initialize writer with first chunk
                if writer is None:
                    writer = pq.ParquetWriter(
                        str(output_path),
                        export_schema,
                        compression='zstd',
                        compression_level=3,
                    )

                # Write chunk
                writer.write_table(table)

                stats.total_rows += rows_in_chunk
                stats.total_chunks += 1

        finally:
            if writer is not None:
                writer.close()

        # Get file size
        if output_path.exists():
            stats.file_size_bytes = output_path.stat().st_size

        logger.info(
            f"Export complete: {stats.total_rows:,} rows in {stats.total_chunks} chunks, "
            f"file size: {stats.file_size_bytes / (1024*1024):.2f} MB"
        )

        return stats

    def _build_filters(
        self,
        file_id: Optional[UUID],
        import_id: Optional[UUID],
        feature_type: Optional[str],
        study_id: Optional[str],
    ) -> str:
        """Build SQL WHERE clause filters.

        Args:
            file_id: Filter by file
            import_id: Filter by import
            feature_type: Filter by feature type
            study_id: Filter by study

        Returns:
            SQL filter string to append to WHERE 1=1
        """
        filters = []

        if file_id:
            filters.append(f"AND f.id = '{file_id}'")

        if import_id:
            filters.append(f"AND f.import_id = '{import_id}'")

        if feature_type:
            # Escape single quotes for safety
            safe_type = feature_type.replace("'", "''")
            filters.append(f"AND ft.feature_type = '{safe_type}'")

        if study_id:
            # Escape single quotes for safety
            safe_study = study_id.replace("'", "''")
            filters.append(f"AND st.study_id = '{safe_study}'")

        return '\n        '.join(filters)

    def _stream_chunks(
        self,
        query: str,
        chunk_size: int
    ) -> Iterator[pd.DataFrame]:
        """Stream query results in chunks.

        Args:
            query: SQL query to execute
            chunk_size: Number of rows per chunk

        Yields:
            DataFrames with chunk_size rows each
        """
        with self.engine.connect() as conn:
            # Use pandas chunked reading
            for chunk_df in pd.read_sql_query(
                text(query),
                conn,
                chunksize=chunk_size,
            ):
                # Convert UUID columns to strings (already done in SQL)
                # Convert timestamps to string for better Parquet compatibility
                if 'created_at' in chunk_df.columns:
                    chunk_df['created_at'] = chunk_df['created_at'].astype(str)

                yield chunk_df

    def get_export_preview(
        self,
        file_id: Optional[UUID] = None,
        import_id: Optional[UUID] = None,
        feature_type: Optional[str] = None,
        study_id: Optional[str] = None,
        limit: int = 10,
    ) -> pd.DataFrame:
        """Get a preview of export data.

        Args:
            file_id: Filter by file
            import_id: Filter by import
            feature_type: Filter by feature type
            study_id: Filter by study
            limit: Maximum rows to return

        Returns:
            DataFrame with preview data
        """
        filters = self._build_filters(file_id, import_id, feature_type, study_id)
        query = self.EXPORT_QUERY.format(filters=filters) + f"\nLIMIT {limit}"

        with self.engine.connect() as conn:
            return pd.read_sql_query(text(query), conn)

    def get_row_count(
        self,
        file_id: Optional[UUID] = None,
        import_id: Optional[UUID] = None,
        feature_type: Optional[str] = None,
        study_id: Optional[str] = None,
    ) -> int:
        """Get count of rows that would be exported.

        Args:
            file_id: Filter by file
            import_id: Filter by import
            feature_type: Filter by feature type
            study_id: Filter by study

        Returns:
            Row count
        """
        filters = self._build_filters(file_id, import_id, feature_type, study_id)

        count_query = f"""
            SELECT COUNT(*) as cnt
            FROM measurements m
            LEFT JOIN files f ON m.file_id = f.id
            LEFT JOIN samples s ON m.sample_uid = s.sample_uid
            LEFT JOIN features ft ON m.feature_uid = ft.feature_uid
            LEFT JOIN studies st ON s.study_pk = st.id
            LEFT JOIN analyses a ON ft.analysis_id = a.analysis_id AND a.study_pk = st.id
            WHERE 1=1
            {filters}
        """

        with self.engine.connect() as conn:
            result = conn.execute(text(count_query))
            return result.scalar()
