import asyncio
import asyncpg
from datetime import datetime
from typing import Optional, List, Dict, Any
import os
from dotenv import load_dotenv

load_dotenv()

class PostgreSQLQueue:
    _tables_initialized = False  # Class-level flag
    
    def __init__(self):
        self.pool = None
        self.host = os.getenv("POSTGRES_HOST", "localhost")
        self.port = int(os.getenv("POSTGRES_PORT", "5432"))
        self.user = os.getenv("POSTGRES_USER", "n8n")
        self.password = os.getenv("POSTGRES_PASSWORD", "password")
        self.database = os.getenv("POSTGRES_DATABASE", "n8n")
    
    async def connect(self, initialize_tables: bool = True):
        """Create connection pool and optionally initialize tables
        
        Args:
            initialize_tables: If True and tables haven't been initialized yet,
                             create the queue tables. Set to False to skip initialization.
        """
        self.pool = await asyncpg.create_pool(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database,
            min_size=2,
            max_size=10
        )
        
        # Initialize queue tables only once (first time connect is called)
        if initialize_tables and not PostgreSQLQueue._tables_initialized:
            await self._initialize_tables()
            PostgreSQLQueue._tables_initialized = True
    
    async def _initialize_tables(self):
        """Create queue tables if they don't exist"""
        async with self.pool.acquire() as conn:
            # Read SQL from queue-setup.sql file
            sql_file = os.path.join(os.path.dirname(__file__), 'queue-setup.sql')
            
            try:
                if os.path.exists(sql_file):
                    with open(sql_file, 'r') as f:
                        sql = f.read()
                        await conn.execute(sql)
                else:
                    # Fallback: create tables inline
                    await conn.execute("""
                        -- Document ingestion queue
                        CREATE TABLE IF NOT EXISTS document_queue (
                            id SERIAL PRIMARY KEY,
                            file_path TEXT NOT NULL,
                            status VARCHAR(50) DEFAULT 'pending',
                            priority INTEGER DEFAULT 0,
                            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                            started_at TIMESTAMP,
                            completed_at TIMESTAMP,
                            error_message TEXT,
                            retry_count INTEGER DEFAULT 0,
                            metadata JSONB
                        );

                        CREATE INDEX IF NOT EXISTS idx_document_queue_status ON document_queue(status);
                        CREATE INDEX IF NOT EXISTS idx_document_queue_priority ON document_queue(priority DESC, created_at ASC);

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

                        CREATE INDEX IF NOT EXISTS idx_query_queue_status ON query_queue(status);
                        CREATE INDEX IF NOT EXISTS idx_query_queue_priority ON query_queue(priority DESC, created_at ASC);
                    """)
            except asyncpg.exceptions.DuplicateTableError:
                # This can happen with concurrent connections trying to create the same index
                # Safe to ignore as it means the table/index already exists
                pass
    
    async def close(self):
        """Close connection pool"""
        if self.pool:
            await self.pool.close()
    
    # Document Queue Operations
    async def enqueue_document(self, file_path: str, priority: int = 0, metadata: Dict = {}):
        """Add document to processing queue"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                INSERT INTO document_queue (file_path, priority, metadata)
                VALUES ($1, $2, $3)
                """,
                file_path, priority, metadata.__str__() or {}
            )
    
    async def dequeue_document(self) -> Optional[Dict[str, Any]]:
        """Get next document from queue (highest priority first)"""
        async with self.pool.acquire() as conn:
            # Use FOR UPDATE SKIP LOCKED for concurrent workers
            row = await conn.fetchrow(
                """
                UPDATE document_queue
                SET status = 'processing',
                    started_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM document_queue
                    WHERE status = 'pending'
                    ORDER BY priority DESC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, file_path, metadata
                """
            )
            return dict(row) if row else None
    
    async def complete_document(self, doc_id: int):
        """Mark document as completed"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE document_queue
                SET status = 'completed',
                    completed_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                doc_id
            )
    
    async def fail_document(self, doc_id: int, error_message: str):
        """Mark document as failed"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE document_queue
                SET status = 'failed',
                    error_message = $2,
                    retry_count = retry_count + 1,
                    completed_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                doc_id, error_message
            )
    
    # Query Queue Operations
    async def enqueue_query(self, query_text: str, priority: int = 0):
        """Add query to processing queue"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                INSERT INTO query_queue (query_text, priority)
                VALUES ($1, $2)
                RETURNING id
                """,
                query_text, priority
            )
            return row['id']
    
    async def dequeue_query(self) -> Optional[Dict[str, Any]]:
        """Get next query from queue"""
        async with self.pool.acquire() as conn:
            row = await conn.fetchrow(
                """
                UPDATE query_queue
                SET status = 'processing',
                    started_at = CURRENT_TIMESTAMP
                WHERE id = (
                    SELECT id FROM query_queue
                    WHERE status = 'pending'
                    ORDER BY priority DESC, created_at ASC
                    FOR UPDATE SKIP LOCKED
                    LIMIT 1
                )
                RETURNING id, query_text
                """
            )
            return dict(row) if row else None
    
    async def complete_query(self, query_id: int, result: Dict):
        """Mark query as completed with result"""
        async with self.pool.acquire() as conn:
            await conn.execute(
                """
                UPDATE query_queue
                SET status = 'completed',
                    result = $2,
                    completed_at = CURRENT_TIMESTAMP
                WHERE id = $1
                """,
                query_id, result
            )
    
    async def get_queue_stats(self) -> Dict[str, int]:
        """Get queue statistics"""
        async with self.pool.acquire() as conn:
            doc_stats = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE status = 'processing') as processing,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed,
                    COUNT(*) FILTER (WHERE status = 'failed') as failed
                FROM document_queue
                """
            )
            query_stats = await conn.fetchrow(
                """
                SELECT 
                    COUNT(*) FILTER (WHERE status = 'pending') as pending,
                    COUNT(*) FILTER (WHERE status = 'processing') as processing,
                    COUNT(*) FILTER (WHERE status = 'completed') as completed
                FROM query_queue
                """
            )
            return {
                'documents': dict(doc_stats),
                'queries': dict(query_stats)
            }