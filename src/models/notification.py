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
    dataset_name: Optional[str] = "mnist"
    batch_size: Optional[int] = 64

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
            client_id=data.get("client_id") or data.get("clientId", ""),
            session_id=data.get("session_id") or data.get("sessionId", ""),
            inputs_format=data.get("inputs_format") or data.get("inputsFormat"),
            outputs_format=data.get("outputs_format") or data.get("outputsFormat"),
            model_type=data.get("model_type") or data.get("modelType"),
            dataset_name=data.get("dataset_name") or data.get("datasetName", "mnist"),
            batch_size=data.get("batch_size") or data.get("batchSize", 64),
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
            "dataset_name": self.dataset_name,
            "batch_size": self.batch_size,
        }

    def validate(self) -> bool:
        """
        Validate that required fields are present.

        Returns:
            True if valid, False otherwise
        """
        return bool(self.client_id and self.session_id)
