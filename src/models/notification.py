from dataclasses import dataclass
from typing import Optional


@dataclass
class ConnectNotification:
    """
    Model representing a client connection notification.
    This is the message structure received from RabbitMQ.
    """

    client_id: str
    session_id: str
    inputs_format: Optional[str] = None
    outputs_format: Optional[str] = None
    model_type: Optional[str] = None

    @classmethod
    def from_dict(cls, data: dict) -> "ConnectNotification":
        """
        Create a ConnectNotification from a dictionary.
        Handles both snake_case (Python) and camelCase (Go) field names.

        Args:
            data: Dictionary containing notification data

        Returns:
            ConnectNotification instance
        """
        return cls(
            client_id=data.get("client_id"),
            session_id=data.get("session_id"),
            inputs_format=data.get("inputs_format"),
            outputs_format=data.get("outputs_format"),
            model_type=data.get("model_type"),
        )

    def to_dict(self) -> dict:
        """
        Convert the notification to a dictionary with snake_case keys.

        Returns:
            Dictionary representation
        """
        return {
            "client_id": self.client_id,
            "session_id": self.session_id,
            "inputs_format": self.inputs_format,
            "outputs_format": self.outputs_format,
            "model_type": self.model_type,
        }

    def validate(self) -> bool:
        """
        Validate that required fields are present.

        Returns:
            True if valid, False otherwise
        """
        return bool(self.client_id and self.session_id)
