"""Add category columns to files table for export

Revision ID: 007
Revises: 006
Create Date: 2026-01-31

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '007'
down_revision: Union[str, None] = '006'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add exposure, sample_type, platform columns to files table."""

    # Add exposure column (OB/CON/NULL)
    op.add_column(
        'files',
        sa.Column('exposure', sa.Text(), nullable=True)
    )

    # Add sample_type column (Serum/Urine/Feces/CSF/NULL)
    op.add_column(
        'files',
        sa.Column('sample_type', sa.Text(), nullable=True)
    )

    # Add platform column (ESI_pos/ESI_neg/HILIC/QQQ/QTOF/etc)
    op.add_column(
        'files',
        sa.Column('platform', sa.Text(), nullable=True)
    )

    # Add indexes for efficient filtering
    op.create_index('idx_files_exposure', 'files', ['exposure'])
    op.create_index('idx_files_sample_type', 'files', ['sample_type'])
    op.create_index('idx_files_platform', 'files', ['platform'])

    # Add index on detected_type if not exists (for type-based queries)
    op.create_index(
        'idx_files_detected_type', 'files', ['detected_type'],
        if_not_exists=True
    )


def downgrade() -> None:
    """Remove category columns from files table."""

    # Drop indexes
    op.drop_index('idx_files_detected_type', 'files', if_exists=True)
    op.drop_index('idx_files_platform', 'files')
    op.drop_index('idx_files_sample_type', 'files')
    op.drop_index('idx_files_exposure', 'files')

    # Drop columns
    op.drop_column('files', 'platform')
    op.drop_column('files', 'sample_type')
    op.drop_column('files', 'exposure')
