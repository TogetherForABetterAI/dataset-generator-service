import pytest
import pika
import psycopg2
import time
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker


@pytest.fixture(scope="session")
def wait_for_services():
    """Wait for RabbitMQ and PostgreSQL to be ready"""
    rabbitmq_ready = False
    postgres_ready = False
    max_attempts = 30

    for attempt in range(max_attempts):
        if not rabbitmq_ready:
            try:
                connection = pika.BlockingConnection(
                    pika.ConnectionParameters(
                        host="localhost",
                        port=5673,
                        credentials=pika.PlainCredentials("guest", "guest"),
                    )
                )
                connection.close()
                rabbitmq_ready = True
                print("✓ RabbitMQ is ready")
            except Exception:
                pass

        if not postgres_ready:
            try:
                conn = psycopg2.connect(
                    host="localhost",
                    port=5434,
                    user="postgres",
                    password="postgres",
                    database="dataset_db",
                )
                conn.close()
                postgres_ready = True
                print("✓ PostgreSQL is ready")
            except Exception:
                pass

        if rabbitmq_ready and postgres_ready:
            time.sleep(2)
            return

        time.sleep(1)

    if not rabbitmq_ready:
        pytest.fail("RabbitMQ did not become ready in time")
    if not postgres_ready:
        pytest.fail("PostgreSQL did not become ready in time")


@pytest.fixture(scope="function")
def rabbitmq_channel(wait_for_services):
    """Provide a RabbitMQ channel for tests"""
    connection = pika.BlockingConnection(
        pika.ConnectionParameters(
            host="localhost",
            port=5673,
            credentials=pika.PlainCredentials("guest", "guest"),
        )
    )
    channel = connection.channel()

    channel.exchange_declare(
        exchange="new_connections_exchange", exchange_type="fanout", durable=True
    )

    channel.exchange_declare(
        exchange="dispatcher_exchange", exchange_type="fanout", durable=True
    )

    channel.queue_declare(queue="generate_data_queue", durable=True)

    channel.queue_bind(queue="generate_data_queue", exchange="new_connections_exchange")

    yield channel

    channel.close()
    connection.close()


@pytest.fixture(scope="function")
def db_session(wait_for_services):
    """Provide a SQLAlchemy session for database operations"""
    engine = create_engine("postgresql://postgres:postgres@localhost:5434/dataset_db")

    with engine.begin() as connection:
        connection.execute(
            text(
                """
            CREATE TABLE IF NOT EXISTS batches (
                session_id VARCHAR(255) NOT NULL,
                batch_index INTEGER NOT NULL,
                data_payload BYTEA NOT NULL,
                labels JSONB NOT NULL,
                is_enqueued BOOLEAN NOT NULL DEFAULT FALSE,
                created_at TIMESTAMP NOT NULL DEFAULT NOW(),
                PRIMARY KEY (session_id, batch_index)
            )
        """
            )
        )
        connection.execute(
            text(
                "CREATE INDEX IF NOT EXISTS idx_batches_session_id ON batches(session_id)"
            )
        )
        connection.execute(
            text(
                'CREATE INDEX IF NOT EXISTS idx_batches_is_enqueued ON batches (is_enqueued)'
            )
        )

    Session = sessionmaker(bind=engine)
    session = Session()

    yield session

    session.close()
    engine.dispose()
