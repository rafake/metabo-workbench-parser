"""Add parse status columns to files table

Revision ID: 006
Revises: 005
Create Date: 2026-01-31

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa

# revision identifiers, used by Alembic.
revision: str = '006'
down_revision: Union[str, None] = '005'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add parse status tracking columns to files table."""

    # Add parse_status column with default 'pending'
    op.add_column(
        'files',
        sa.Column('parse_status', sa.Text(), nullable=False, server_default='pending')
    )

    # Add parse_error column for storing error messages
    op.add_column(
        'files',
        sa.Column('parse_error', sa.Text(), nullable=True)
    )

    # Add parsed_at timestamp
    op.add_column(
        'files',
        sa.Column('parsed_at', sa.TIMESTAMP(timezone=True), nullable=True)
    )

    # Add index for parse_status to enable efficient filtering
    op.create_index('idx_files_parse_status', 'files', ['parse_status'])

    # Add check constraint for valid parse_status values
    op.create_check_constraint(
        'ck_files_parse_status',
        'files',
        "parse_status IN ('pending', 'success', 'failed', 'skipped')"
    )


def downgrade() -> None:
    """Remove parse status tracking columns from files table."""

    # Drop check constraint
    op.drop_constraint('ck_files_parse_status', 'files', type_='check')

    # Drop index
    op.drop_index('idx_files_parse_status', 'files')

    # Drop columns
    op.drop_column('files', 'parsed_at')
    op.drop_column('files', 'parse_error')
    op.drop_column('files', 'parse_status')
