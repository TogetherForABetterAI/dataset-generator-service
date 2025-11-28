"""
NotifyDispatcher model for dispatcher notification messages.
"""

from dataclasses import dataclass
from typing import Dict, Any


@dataclass
class NotifyDispatcher:
    """
    Notification message sent to the dispatcher after batch generation completes.

    Attributes:
        user_id: ID of the client that requested the data
        session_id: ID of the session/connection
        total_batches_generated: Total number of batches generated for this session
    """

    user_id: str
    session_id: str
    total_batches_generated: int = 0

    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.

        Returns:
            Dictionary representation
        """
        return {
            "user_id": self.user_id,
            "session_id": self.session_id,
            "total_batches_generated": self.total_batches_generated,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NotifyDispatcher":
        """
        Create instance from dictionary.

        Args:
            data: Dictionary with user_id, session_id, and total_batches_generated

        Returns:
            NotifyDispatcher instance
        """
        return cls(
            user_id=data.get("user_id", ""),
            session_id=data.get("session_id", ""),
            total_batches_generated=data.get("total_batches_generated", 0),
        )

    def validate(self) -> bool:
        """
        Validate that required fields are present.

        Returns:
            True if valid, False otherwise
        """
        return bool(self.user_id) and bool(self.session_id)
