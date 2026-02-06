import asyncio
import uuid
from temporalio.client import Client
from shared import DocumentDetails,DocumentState, DOCUMENT_TASK_QUEUE, QUERY_TASK_QUEUE

async def main():
    client = await Client.connect("localhost:7233")
    document_result = await client.execute_workflow(
        workflow="DocumentIngestion",
        namespace="Temporal",
        id=f"ingest-document-workflow-{uuid.uuid4()}",
        task_queue=DOCUMENT_TASK_QUEUE,
    )
    query_result = await client.execute_workflow(
        workflow="QueryProcessing",
        namespace="Temporal",
        id=f"ingest-query-workflow-{uuid.uuid4()}",
        task_queue=QUERY_TASK_QUEUE,
    )
    print("Workflow result:", document_result)
    print("Workflow result:", query_result)

if __name__ == "__main__":
    asyncio.run(main())