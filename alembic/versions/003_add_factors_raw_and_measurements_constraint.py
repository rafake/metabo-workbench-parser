"""Add factors_raw, file_id, and measurements unique constraint

Revision ID: 003
Revises: 002
Create Date: 2026-01-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '003'
down_revision: Union[str, None] = '002'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add new columns and constraints."""
    
    # Add factors_raw to samples table
    op.add_column('samples', sa.Column('factors_raw', sa.Text(), nullable=True))
    
    # Add file_id to analyses table (FK to files)
    op.add_column('analyses', sa.Column('file_id', postgresql.UUID(as_uuid=True), nullable=True))
    op.create_foreign_key(
        'fk_analyses_file_id', 
        'analyses', 
        'files', 
        ['file_id'], 
        ['id'],
        ondelete='SET NULL'
    )
    
    # Add unique constraint on measurements (sample_uid, feature_uid)
    # First, clean up any potential duplicates by keeping only the latest
    op.execute("""
        DELETE FROM measurements m1
        USING measurements m2
        WHERE m1.id < m2.id
        AND m1.sample_uid = m2.sample_uid
        AND m1.feature_uid = m2.feature_uid
    """)
    
    op.create_unique_constraint(
        'uq_measurement_sample_feature',
        'measurements',
        ['sample_uid', 'feature_uid']
    )


def downgrade() -> None:
    """Remove columns and constraints."""
    op.drop_constraint('uq_measurement_sample_feature', 'measurements', type_='unique')
    op.drop_constraint('fk_analyses_file_id', 'analyses', type_='foreignkey')
    op.drop_column('analyses', 'file_id')
    op.drop_column('samples', 'factors_raw')
