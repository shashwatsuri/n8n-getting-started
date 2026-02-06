import asyncio
from pathlib import Path
import sys

# Add src directory to path
src_dir = Path(__file__).parent.parent / "src"
sys.path.insert(0, str(src_dir))

from unstructured2graph.queue_manager import PostgreSQLQueue
import glob

async def enqueue_pdfs():
    queue = PostgreSQLQueue()
    await queue.connect()
    
    # Add all PDFs from a directory
    pdf_files = glob.glob("/Users/bitovi/Repos/n8n-getting-started/unstructured2graph/documents/*.pdf")
    
    for pdf_file in pdf_files:
        await queue.enqueue_document(pdf_file, priority=1)
        print(f"Enqueued: {pdf_file}")
    
    stats = await queue.get_queue_stats()
    print(f"\nQueue Stats: {stats}")
    
    await queue.close()

if __name__ == "__main__":
    asyncio.run(enqueue_pdfs())