"""SQLAlchemy models for database tables."""

import uuid
from datetime import datetime, timezone
from typing import Optional

from sqlalchemy import (
    Column,
    String,
    Text,
    BigInteger,
    Float,
    ForeignKey,
    UniqueConstraint,
    Index,
    CheckConstraint,
)
from sqlalchemy.dialects.postgresql import UUID, TIMESTAMP
from sqlalchemy.orm import relationship

from metaloader.database import Base


def utc_now():
    """Get current UTC timestamp."""
    return datetime.now(timezone.utc)


class Import(Base):
    """Import registry table."""

    __tablename__ = "imports"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)
    root_path = Column(Text, nullable=True)
    status = Column(Text, nullable=False, default="running")
    notes = Column(Text, nullable=True)

    # Relationships
    files = relationship("File", back_populates="import_record", cascade="all, delete-orphan")

    __table_args__ = (
        CheckConstraint(
            "status IN ('running', 'success', 'failed')", name="valid_import_status"
        ),
    )


class File(Base):
    """Files table with deduplication."""

    __tablename__ = "files"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    import_id = Column(UUID(as_uuid=True), ForeignKey("imports.id", ondelete="CASCADE"), nullable=False)
    path_rel = Column(Text, nullable=True)
    path_abs = Column(Text, nullable=False)
    filename = Column(Text, nullable=False)
    ext = Column(Text, nullable=False)
    size_bytes = Column(BigInteger, nullable=False)
    sha256 = Column(String(64), nullable=False)
    detected_type = Column(Text, nullable=False)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)

    # Relationships
    import_record = relationship("Import", back_populates="files")

    __table_args__ = (
        UniqueConstraint("sha256", "size_bytes", name="uq_file_sha256_size"),
        Index("idx_file_sha256", "sha256"),
    )


class Study(Base):
    """Studies placeholder table."""

    __tablename__ = "studies"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    study_id = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)

    # Relationships
    analyses = relationship("Analysis", back_populates="study")
    samples = relationship("Sample", back_populates="study")


class Analysis(Base):
    """Analyses placeholder table."""

    __tablename__ = "analyses"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    study_pk = Column(UUID(as_uuid=True), ForeignKey("studies.id"), nullable=True)
    analysis_id = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)

    # Relationships
    study = relationship("Study", back_populates="analyses")


class Sample(Base):
    """Samples placeholder table."""

    __tablename__ = "samples"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    study_pk = Column(UUID(as_uuid=True), ForeignKey("studies.id"), nullable=True)
    sample_label = Column(Text, nullable=True)
    sample_uid = Column(Text, unique=True, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)

    # Relationships
    study = relationship("Study", back_populates="samples")
    measurements = relationship("Measurement", back_populates="sample")


class Feature(Base):
    """Features placeholder table."""

    __tablename__ = "features"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    feature_uid = Column(Text, unique=True, nullable=True)
    feature_type = Column(Text, nullable=True)
    name_raw = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)

    # Relationships
    measurements = relationship("Measurement", back_populates="feature")


class Measurement(Base):
    """Measurements placeholder table."""

    __tablename__ = "measurements"

    id = Column(UUID(as_uuid=True), primary_key=True, default=uuid.uuid4)
    sample_uid = Column(Text, ForeignKey("samples.sample_uid"), nullable=True)
    feature_uid = Column(Text, ForeignKey("features.feature_uid"), nullable=True)
    value = Column(Float, nullable=True)
    unit = Column(Text, nullable=True)
    created_at = Column(TIMESTAMP(timezone=True), default=utc_now, nullable=False)

    # Relationships
    sample = relationship("Sample", back_populates="measurements")
    feature = relationship("Feature", back_populates="measurements")

    __table_args__ = (
        Index("idx_measurement_sample", "sample_uid"),
        Index("idx_measurement_feature", "feature_uid"),
    )
