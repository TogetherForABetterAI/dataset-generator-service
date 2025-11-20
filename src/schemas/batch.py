"""
SQLAlchemy models for the database schema.
"""

from sqlalchemy import Column, Integer, String, LargeBinary, Boolean, TIMESTAMP, Index
from sqlalchemy.dialects.postgresql import JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.sql import func

Base = declarative_base()


class Batch(Base):
    """
    Batch model representing a single batch of training data.
    """

    __tablename__ = "batches"

    session_id = Column(String(255), primary_key=True, nullable=False)
    batch_index = Column(Integer, primary_key=True, nullable=False)
    data_payload = Column(LargeBinary, nullable=False)
    labels = Column(JSON, nullable=False)
    isEnqueued = Column(Boolean, nullable=False, default=False)
    created_at = Column(TIMESTAMP, nullable=False, server_default=func.now())

    # Indexes
    __table_args__ = (
        Index("idx_batches_session_id", "session_id"),
        Index("idx_batches_isenqueued", "isEnqueued"),
    )

    def __repr__(self):
        return f"<Batch(session_id={self.session_id}, batch_index={self.batch_index})>"
