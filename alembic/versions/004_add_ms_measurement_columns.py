"""Add MS measurement columns: file_id, col_index, replicate_ix

Revision ID: 004
Revises: 003
Create Date: 2026-01-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '004'
down_revision: Union[str, None] = '003'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add file_id, col_index, replicate_ix to measurements and change unique constraint."""

    # Add new columns to measurements
    op.add_column('measurements', sa.Column('file_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.add_column('measurements', sa.Column('col_index', sa.Integer(), nullable=True))
    op.add_column('measurements', sa.Column('replicate_ix', sa.SmallInteger(), nullable=True))

    # Add FK constraint for file_id
    op.create_foreign_key(
        'fk_measurements_file_id',
        'measurements',
        'files',
        ['file_id'],
        ['id'],
        ondelete='CASCADE'
    )

    # Add index on file_id for faster lookups
    op.create_index('idx_measurement_file_id', 'measurements', ['file_id'])

    # Add new unique constraint for MS data
    # This allows same sample+feature from different files/columns
    # Using partial index: only when file_id is NOT NULL
    op.execute("""
        CREATE UNIQUE INDEX uq_measurement_file_col_feature
        ON measurements (file_id, col_index, feature_uid)
        WHERE file_id IS NOT NULL
    """)

    # Add refmet column to features (optional, for RefMet mapping)
    op.add_column('features', sa.Column('refmet_name', sa.Text(), nullable=True))

    # Add analysis_id to features for grouping
    op.add_column('features', sa.Column('analysis_id', sa.Text(), nullable=True))
    op.create_index('idx_feature_analysis_id', 'features', ['analysis_id'])


def downgrade() -> None:
    """Remove MS measurement columns."""

    # Drop index and constraint
    op.drop_index('idx_feature_analysis_id', 'features')
    op.drop_column('features', 'analysis_id')
    op.drop_column('features', 'refmet_name')

    op.execute("DROP INDEX IF EXISTS uq_measurement_file_col_feature")
    op.drop_index('idx_measurement_file_id', 'measurements')
    op.drop_constraint('fk_measurements_file_id', 'measurements', type_='foreignkey')

    op.drop_column('measurements', 'replicate_ix')
    op.drop_column('measurements', 'col_index')
    op.drop_column('measurements', 'file_id')
