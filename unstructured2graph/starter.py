import asyncio
import uuid
from temporalio.client import Client
from shared import DocumentDetails, DOCUMENT_TASK_QUEUE, QUERY_TASK_QUEUE
from workflows import DocumentIngestion, QueryProcessing

async def main():
    client = await Client.connect("localhost:7233")
    
    document_details = DocumentDetails(
        id=1,
        name="Static IP address Feature request-v2.pdf",
        file_path="/Users/bitovi/Repos/n8n-getting-started/unstructured2graph/documents/Static IP address Feature request-v2.pdf",
    )

    document_result = await client.execute_workflow(
        workflow=DocumentIngestion.run,
        args=[document_details],
        id=f"ingest-document-workflow-{uuid.uuid4()}",
        task_queue=DOCUMENT_TASK_QUEUE,
    )
    # query_result = await client.execute_workflow(
    #     workflow=QueryProcessing.run,
    #     id=f"ingest-query-workflow-{uuid.uuid4()}",
    #     task_queue=QUERY_TASK_QUEUE,
    # )
    print("Workflow result:", document_result)
    # print("Workflow result:", query_result)

if __name__ == "__main__":
    asyncio.run(main())