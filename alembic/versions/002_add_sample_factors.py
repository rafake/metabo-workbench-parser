"""Add sample_factors table

Revision ID: 002
Revises: 001
Create Date: 2026-01-29

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '002'
down_revision: Union[str, None] = '001'
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Add sample_factors table."""
    
    # Create sample_factors table
    op.create_table(
        'sample_factors',
        sa.Column('id', sa.BigInteger(), nullable=False, autoincrement=True),
        sa.Column('sample_uid', sa.Text(), nullable=False),
        sa.Column('factor_key', sa.Text(), nullable=False),
        sa.Column('factor_value', sa.Text(), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['sample_uid'], ['samples.sample_uid'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('sample_uid', 'factor_key', name='uq_sample_factor')
    )
    
    # Create index on sample_uid for faster lookups
    op.create_index('idx_sample_factors_sample_uid', 'sample_factors', ['sample_uid'])


def downgrade() -> None:
    """Drop sample_factors table."""
    op.drop_index('idx_sample_factors_sample_uid', table_name='sample_factors')
    op.drop_table('sample_factors')
