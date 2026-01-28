"""Initial schema

Revision ID: 001
Revises: 
Create Date: 2026-01-28

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = '001'
down_revision: Union[str, None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """Create all tables."""
    
    # Create imports table
    op.create_table(
        'imports',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.Column('root_path', sa.Text(), nullable=True),
        sa.Column('status', sa.Text(), nullable=False),
        sa.Column('notes', sa.Text(), nullable=True),
        sa.CheckConstraint("status IN ('running', 'success', 'failed')", name='valid_import_status'),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create studies table
    op.create_table(
        'studies',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('study_id', sa.Text(), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create features table
    op.create_table(
        'features',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('feature_uid', sa.Text(), nullable=True),
        sa.Column('feature_type', sa.Text(), nullable=True),
        sa.Column('name_raw', sa.Text(), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('feature_uid')
    )
    
    # Create files table
    op.create_table(
        'files',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('import_id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('path_rel', sa.Text(), nullable=True),
        sa.Column('path_abs', sa.Text(), nullable=False),
        sa.Column('filename', sa.Text(), nullable=False),
        sa.Column('ext', sa.Text(), nullable=False),
        sa.Column('size_bytes', sa.BigInteger(), nullable=False),
        sa.Column('sha256', sa.String(length=64), nullable=False),
        sa.Column('detected_type', sa.Text(), nullable=False),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['import_id'], ['imports.id'], ondelete='CASCADE'),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('sha256', 'size_bytes', name='uq_file_sha256_size')
    )
    op.create_index('idx_file_sha256', 'files', ['sha256'])
    
    # Create analyses table
    op.create_table(
        'analyses',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('study_pk', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('analysis_id', sa.Text(), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['study_pk'], ['studies.id']),
        sa.PrimaryKeyConstraint('id')
    )
    
    # Create samples table
    op.create_table(
        'samples',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('study_pk', postgresql.UUID(as_uuid=True), nullable=True),
        sa.Column('sample_label', sa.Text(), nullable=True),
        sa.Column('sample_uid', sa.Text(), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['study_pk'], ['studies.id']),
        sa.PrimaryKeyConstraint('id'),
        sa.UniqueConstraint('sample_uid')
    )
    
    # Create measurements table
    op.create_table(
        'measurements',
        sa.Column('id', postgresql.UUID(as_uuid=True), nullable=False),
        sa.Column('sample_uid', sa.Text(), nullable=True),
        sa.Column('feature_uid', sa.Text(), nullable=True),
        sa.Column('value', sa.Float(), nullable=True),
        sa.Column('unit', sa.Text(), nullable=True),
        sa.Column('created_at', postgresql.TIMESTAMP(timezone=True), nullable=False),
        sa.ForeignKeyConstraint(['feature_uid'], ['features.feature_uid']),
        sa.ForeignKeyConstraint(['sample_uid'], ['samples.sample_uid']),
        sa.PrimaryKeyConstraint('id')
    )
    op.create_index('idx_measurement_sample', 'measurements', ['sample_uid'])
    op.create_index('idx_measurement_feature', 'measurements', ['feature_uid'])


def downgrade() -> None:
    """Drop all tables."""
    op.drop_index('idx_measurement_feature', table_name='measurements')
    op.drop_index('idx_measurement_sample', table_name='measurements')
    op.drop_table('measurements')
    op.drop_table('samples')
    op.drop_table('analyses')
    op.drop_index('idx_file_sha256', table_name='files')
    op.drop_table('files')
    op.drop_table('features')
    op.drop_table('studies')
    op.drop_table('imports')
