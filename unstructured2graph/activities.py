import asyncio

from temporalio import activity
from shared import DocumentDetails,QueryDetails, DOCUMENT_TASK_QUEUE, QUERY_TASK_QUEUE

@activity.defn
async def process_document(document_details: DocumentDetails) -> str:
    """Activity to process a document and return a result"""
    # Simulate processing time
    await asyncio.sleep(5)
    
    # Here you would add your actual document processing logic
    # For this example, we'll just return a success message
    return f"Processed document: {document_details.name}"


@activity.defn
async def process_query(query_details: QueryDetails) -> str:
    """Activity to process a query and return a result"""
    # Simulate processing time
    await asyncio.sleep(3)
    
    # Here you would add your actual query processing logic
    # For this example, we'll just return a success message
    return f"Processed query: {query_details.name}"