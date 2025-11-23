-- Database initialization script for dataset-generator-service
-- Creates the batches table with proper indexes

CREATE TABLE IF NOT EXISTS batches (
    session_id VARCHAR(255) NOT NULL,
    batch_index INTEGER NOT NULL,
    data_payload BYTEA NOT NULL,
    labels JSONB NOT NULL,
    "isEnqueued" BOOLEAN NOT NULL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT NOW(),
    PRIMARY KEY (session_id, batch_index)
);

-- Create indexes for better query performance
CREATE INDEX IF NOT EXISTS idx_batches_session_id ON batches (session_id);
CREATE INDEX IF NOT EXISTS idx_batches_isenqueued ON batches ("isEnqueued");

-- Display confirmation
SELECT 'Database initialized successfully' AS status;
