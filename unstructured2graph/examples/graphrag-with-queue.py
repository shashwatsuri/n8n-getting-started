import asyncio
import logging
from memgraph_toolbox.api.memgraph import Memgraph
import sys
from pathlib import Path
import os

# Add src directory to path
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))

from unstructured2graph.queue_manager import PostgreSQLQueue
from loading import from_unstructured_with_prep

from dotenv import load_dotenv

load_dotenv()

username = os.getenv("MEMGRAPH_USERNAME", "memgraph")
password = os.getenv("MEMGRAPH_PASSWORD", "memgraph")
url = os.getenv("MEMGRAPH_URI", "localhost")
database = os.getenv("MEMGRAPH_DATABASE", "memgraph")
openai_api_key = os.getenv("OPENAI_API_KEY", "")


logging.basicConfig(level=logging.INFO)

async def document_worker(queue: PostgreSQLQueue):
    """Worker that processes documents from the queue"""
    while True:
        doc = await queue.dequeue_document()
        if not doc:
            await asyncio.sleep(5)  # Wait if queue is empty
            continue
        
        try:
            logging.info(f"Processing document {doc['id']}: {doc['file_path']}")
            
            # Process the document with the file path from the queue
            await from_unstructured_with_prep([doc['file_path']])
            
            await queue.complete_document(doc['id'])
            logging.info(f"Completed document {doc['id']}")
            
        except Exception as e:
            logging.error(f"Failed to process document {doc['id']}: {e}")
            await queue.fail_document(doc['id'], str(e))

async def query_worker(queue: PostgreSQLQueue):
    """Worker that processes queries from the queue"""
    while True:
        memgraph = Memgraph(username=username, password=password, url=url, database=database)
        query = await queue.dequeue_query()
        if not query:
            await asyncio.sleep(2)
            continue
        
        try:
            logging.info(f"Processing query {query['id']}: {query['query_text']}")
            
            # Run GraphRAG query
            result = await memgraph.query(query['query_text'])
            await queue.complete_query(query['id'], result)
            logging.info(f"Completed query {query['id']}")
            
        except Exception as e:
            logging.error(f"Failed to process query {query['id']}: {e}")

async def main():
    queue = PostgreSQLQueue()
    await queue.connect()
    
    try:
        # Show queue stats
        stats = await queue.get_queue_stats()
        print(f"Queue Stats: {stats}")
        
        # Start workers
        workers = [
            asyncio.create_task(document_worker(queue)),
            asyncio.create_task(query_worker(queue)),
        ]
        
        await asyncio.gather(*workers)
        
    finally:
        await queue.close()

if __name__ == "__main__":
    asyncio.run(main())
