"""Add category columns: exposure, sample_matrix, device

Revision ID: 005
Revises: 004
Create Date: 2026-01-30

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '005'
down_revision: Union[str, None] = '004'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add category columns for exposure, sample_matrix, and device."""

    # Add columns to samples table
    op.add_column('samples', sa.Column('exposure', sa.Text(), nullable=True))
    op.add_column('samples', sa.Column('sample_matrix', sa.Text(), nullable=True))

    # Add indexes for samples category columns
    op.create_index('idx_samples_exposure', 'samples', ['exposure'])
    op.create_index('idx_samples_sample_matrix', 'samples', ['sample_matrix'])

    # Add device column to files table
    op.add_column('files', sa.Column('device', sa.Text(), nullable=True))
    op.create_index('idx_files_device', 'files', ['device'])

    # Also add device to analyses for convenience
    op.add_column('analyses', sa.Column('device', sa.Text(), nullable=True))
    op.create_index('idx_analyses_device', 'analyses', ['device'])

    # Create the v_long_measurements view for R export
    op.execute("""
        CREATE OR REPLACE VIEW public.v_long_measurements AS
        SELECT
            m.id AS measurement_id,
            s.sample_uid,
            s.sample_label,
            s.exposure,
            s.sample_matrix,
            f.id AS file_id,
            f.device,
            f.detected_type,
            f.filename,
            ft.feature_uid,
            ft.feature_type,
            ft.name_raw AS feature,
            ft.refmet_name,
            m.value,
            m.unit,
            m.col_index,
            m.replicate_ix,
            st.study_id,
            a.analysis_id
        FROM measurements m
        LEFT JOIN samples s ON m.sample_uid = s.sample_uid
        LEFT JOIN features ft ON m.feature_uid = ft.feature_uid
        LEFT JOIN files f ON m.file_id = f.id
        LEFT JOIN studies st ON s.study_pk = st.id
        LEFT JOIN analyses a ON ft.analysis_id = a.analysis_id AND a.study_pk = st.id
    """)


def downgrade() -> None:
    """Remove category columns and view."""

    # Drop the view
    op.execute("DROP VIEW IF EXISTS public.v_long_measurements")

    # Drop analyses columns
    op.drop_index('idx_analyses_device', 'analyses')
    op.drop_column('analyses', 'device')

    # Drop files columns
    op.drop_index('idx_files_device', 'files')
    op.drop_column('files', 'device')

    # Drop samples columns
    op.drop_index('idx_samples_sample_matrix', 'samples')
    op.drop_index('idx_samples_exposure', 'samples')
    op.drop_column('samples', 'sample_matrix')
    op.drop_column('samples', 'exposure')
