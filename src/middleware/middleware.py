import pika
import time
import logging
import json
from typing import Dict, Any, Optional, Callable
from src.config.config import (
    CONSUME_QUEUE,
    DISPATCHER_EXCHANGE,
    NEW_CONNECTIONS_EXCHANGE,
    MiddlewareConfig,
)

logger = logging.getLogger(__name__)


class Middleware:
    """
    Middleware to handle all RabbitMQ interactions including:
    - Connection management with retry logic
    - Queue and exchange declaration
    - Message publishing with AMQP transactions
    - Message consumption
    """

    def __init__(self, config: MiddlewareConfig):
        self.config = config
        self.connection: Optional[pika.BlockingConnection] = None
        self.channel: Optional[pika.channel.Channel] = None

    def connect(self) -> pika.BlockingConnection:
        """
        Establish connection to RabbitMQ with retry logic
        Returns the connection object
        """
        credentials = pika.PlainCredentials(self.config.username, self.config.password)
        parameters = pika.ConnectionParameters(
            host=self.config.host,
            port=self.config.port,
            credentials=credentials,
            heartbeat=600,
            blocked_connection_timeout=300,
        )

        retry_count = 0
        while retry_count < self.config.max_retries:
            try:
                logger.info(
                    f"Attempting to connect to RabbitMQ at {self.config.host}:{self.config.port} (attempt {retry_count + 1}/{self.config.max_retries})"
                )
                connection = pika.BlockingConnection(parameters)
                logger.info("Successfully connected to RabbitMQ")
                return connection
            except Exception as e:
                retry_count += 1
                if retry_count >= self.config.max_retries:
                    logger.error(
                        f"Failed to connect to RabbitMQ after {self.config.max_retries} attempts"
                    )
                    raise
                wait_time = min(2**retry_count, 30)  # Exponential backoff, max 30s
                logger.warning(
                    f"Failed to connect to RabbitMQ: {e}. Retrying in {wait_time}s..."
                )
                time.sleep(wait_time)

        raise Exception("Failed to connect to RabbitMQ")

    def create_channel(
        self, connection: Optional[pika.BlockingConnection] = None
    ) -> pika.channel.Channel:
        """
        Create a new channel from a connection.
        Each worker process should have its own exclusive channel.
        """
        if connection is None:
            if self.connection is None:
                self.connection = self.connect()
            connection = self.connection

        channel = connection.channel()
        logger.info(f"Created new channel: {channel.channel_number}")
        return channel

    def declare_topology(self, channel: pika.channel.Channel):
        """
        Declare the complete RabbitMQ topology:
        - Exchanges
        - Queues
        - Bindings

        This should be called once at startup.
        """
        try:
            # Declare exchanges
            logger.info("Declaring exchange: new_connections")
            channel.exchange_declare(
                exchange=NEW_CONNECTIONS_EXCHANGE, exchange_type="fanout", durable=True
            )

            logger.info("Declaring exchange: dispatcher")
            channel.exchange_declare(
                exchange=DISPATCHER_EXCHANGE, exchange_type="fanout", durable=True
            )

            # Declare the main queue for this service
            logger.info("Declaring queue: generate_data_queue")
            channel.queue_declare(queue=CONSUME_QUEUE, durable=True)

            # Bind queue to exchange
            logger.info("Binding generate_data_queue to new_connections exchange")
            channel.queue_bind(
                exchange=NEW_CONNECTIONS_EXCHANGE,
                queue=CONSUME_QUEUE,
                routing_key="",
            )

            logger.info("Topology declared successfully")

        except Exception as e:
            logger.error(f"Failed to declare topology: {e}")
            raise

    def set_qos(self, channel: pika.channel.Channel, prefetch_count: int = 1):
        """
        Set Quality of Service (QoS) for fair message distribution.

        Args:
            channel: RabbitMQ channel
            prefetch_count: Number of unacknowledged messages per consumer (default: 1)
        """
        try:
            channel.basic_qos(prefetch_count=prefetch_count)
            logger.info(f"Set prefetch_count to {prefetch_count}")
        except Exception as e:
            logger.error(f"Failed to set QoS: {e}")
            raise

    def publish_with_transaction(
        self,
        channel: pika.channel.Channel,
        exchange: str,
        routing_key: str,
        message: Dict[str, Any],
        delivery_tag: int,
    ):
        """
        Publish a message and ACK the original message atomically using AMQP transactions.

        Args:
            channel: The channel to use (must be exclusive to this operation)
            exchange: The exchange to publish to
            routing_key: The routing key
            message: The message payload as a dictionary
            delivery_tag: The delivery tag of the original message to ACK
        """
        try:
            # Start transaction
            channel.tx_select()
            logger.debug(f"Started transaction on channel {channel.channel_number}")

            # Serialize message to JSON
            message_body = json.dumps(message)

            # Publish message
            channel.basic_publish(
                exchange=exchange,
                routing_key=routing_key,
                body=message_body,
                properties=pika.BasicProperties(
                    delivery_mode=2,  # Make message persistent
                    content_type="application/json",
                ),
            )
            logger.debug(
                f"Published message to exchange={exchange}, routing_key={routing_key}"
            )

            # ACK the original message
            channel.basic_ack(delivery_tag=delivery_tag)
            logger.debug(f"ACKed message with delivery_tag={delivery_tag}")

            # Commit transaction
            channel.tx_commit()
            logger.info(
                f"Transaction committed successfully (exchange={exchange}, routing_key={routing_key})"
            )

        except Exception as e:
            logger.error(f"Transaction failed: {e}")
            try:
                # Rollback transaction on error
                channel.tx_rollback()
                logger.warning("Transaction rolled back")
            except Exception as rollback_error:
                logger.error(f"Failed to rollback transaction: {rollback_error}")
            raise

    def consume(
        self,
        channel: pika.channel.Channel,
        queue: str,
        callback: Callable,
        auto_ack: bool = False,
        consumer_tag: str = "",
    ) -> str:
        """
        Start consuming messages from a queue.

        Args:
            channel: The channel to use
            queue: The queue name
            callback: Callback function to process messages
            auto_ack: Whether to auto-acknowledge messages (should be False for manual ACK)
            consumer_tag: Custom consumer tag (e.g., POD_NAME). If empty, RabbitMQ auto-generates one.

        Returns:
            consumer_tag: The consumer tag assigned by RabbitMQ or the provided one
        """
        logger.info(
            f"Starting to consume from queue: {queue} with consumer_tag: {consumer_tag or 'auto-generated'}"
        )
        consumer_tag = channel.basic_consume(
            queue=queue,
            on_message_callback=callback,
            auto_ack=auto_ack,
            consumer_tag=consumer_tag,  # Pass custom tag or empty for auto-generation
        )

        logger.info(f"Consumer started with tag: {consumer_tag}")
        return consumer_tag

    def start_consuming(self, channel: pika.channel.Channel):
        """
        Start the blocking consume loop.
        This will block until stop_consuming is called.

        Args:
            channel: The channel to consume on
        """
        try:
            logger.info("Waiting for messages. To exit press CTRL+C")
            channel.start_consuming()
        except KeyboardInterrupt:
            logger.info("Interrupted by user, stopping consumption")
            channel.stop_consuming()
        except Exception as e:
            logger.error(f"Error during consumption: {e}")
            raise

    def stop_consuming(self, consumer_tag: str):
        """
        Stop consuming messages for a specific consumer.
        This allows graceful shutdown by preventing new messages from being consumed.

        Args:
            consumer_tag: The consumer tag to stop)
        """
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.basic_cancel(consumer_tag)
                logger.info(f"Consumer {consumer_tag} stopped successfully")
            else:
                logger.warning("Channel is already closed, cannot stop consuming")
        except Exception as e:
            logger.error(f"Error stopping consumer {consumer_tag}: {e}")
            raise

    def ack_message(self, channel: pika.channel.Channel, delivery_tag: int):
        """
        Acknowledge a message.

        Args:
            channel: The channel to use
            delivery_tag: The delivery tag of the message to ACK
        """
        try:
            if channel and not channel.is_closed:
                channel.basic_ack(delivery_tag=delivery_tag)
                logger.debug(f"ACKed message with delivery_tag={delivery_tag}")
            else:
                logger.warning(f"Cannot ACK message: channel is closed")
        except Exception as e:
            logger.error(f"Failed to ACK message {delivery_tag}: {e}")
            raise

    def nack_message(
        self, channel: pika.channel.Channel, delivery_tag: int, requeue: bool = False
    ):
        """
        Negative acknowledge a message (NACK).

        Args:
            channel: The channel to use
            delivery_tag: The delivery tag of the message to NACK
            requeue: Whether to requeue the message (True) or discard it (False)
        """
        try:
            if channel and not channel.is_closed:
                channel.basic_nack(delivery_tag=delivery_tag, requeue=requeue)
                logger.debug(
                    f"NACKed message with delivery_tag={delivery_tag}, requeue={requeue}"
                )
            else:
                logger.warning(f"Cannot NACK message: channel is closed")
        except Exception as e:
            logger.error(
                f"Failed to NACK message {delivery_tag} (requeue={requeue}): {e}"
            )
            raise

    def close(self):
        """Close channel and connection gracefully"""
        try:
            if self.channel and not self.channel.is_closed:
                self.channel.close()
                logger.info("Channel closed")
        except Exception as e:
            logger.warning(f"Error closing channel: {e}")

        try:
            if self.connection and not self.connection.is_closed:
                self.connection.close()
                logger.info("Connection closed")
        except Exception as e:
            logger.warning(f"Error closing connection: {e}")
