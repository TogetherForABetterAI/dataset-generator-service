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
        client_id: ID of the client that requested the data
        session_id: ID of the session/connection
    """
    
    client_id: str
    session_id: str
    
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert to dictionary for JSON serialization.
        
        Returns:
            Dictionary representation
        """
        return {
            "client_id": self.client_id,
            "session_id": self.session_id,
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "NotifyDispatcher":
        """
        Create instance from dictionary.
        
        Args:
            data: Dictionary with client_id and session_id
            
        Returns:
            NotifyDispatcher instance
        """
        return cls(
            client_id=data.get("client_id", ""),
            session_id=data.get("session_id", ""),
        )
    
    def validate(self) -> bool:
        """
        Validate that required fields are present.
        
        Returns:
            True if valid, False otherwise
        """
        return bool(self.client_id) and bool(self.session_id)
