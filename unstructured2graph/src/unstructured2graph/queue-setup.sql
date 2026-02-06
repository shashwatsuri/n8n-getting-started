-- Document ingestion queue
CREATE TABLE IF NOT EXISTS document_queue (
    id SERIAL PRIMARY KEY,
    file_path TEXT NOT NULL,
    status VARCHAR(50) DEFAULT 'pending', -- pending, processing, completed, failed
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0,
    metadata JSONB
);

CREATE INDEX idx_document_queue_status ON document_queue(status);
CREATE INDEX idx_document_queue_priority ON document_queue(priority DESC, created_at ASC);

-- Query queue for GraphRAG queries
CREATE TABLE IF NOT EXISTS query_queue (
    id SERIAL PRIMARY KEY,
    query_text TEXT NOT NULL,
    status VARCHAR(50) DEFAULT 'pending',
    priority INTEGER DEFAULT 0,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    result JSONB,
    error_message TEXT,
    retry_count INTEGER DEFAULT 0
);

CREATE INDEX idx_query_queue_status ON query_queue(status);
CREATE INDEX idx_query_queue_priority ON query_queue(priority DESC, created_at ASC);