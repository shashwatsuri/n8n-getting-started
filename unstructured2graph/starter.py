import asyncio
import uuid
from temporalio.client import Client
from shared import DocumentDetails, DOCUMENT_TASK_QUEUE
from workflows import DocumentIngestion

async def main():
    client = await Client.connect("localhost:7233")
    
    document_details = [
        DocumentDetails(
            id=1,
            name="NSA Combo Testing Functionality v000_0_1 (1).pdf",
            file_path="/Users/bitovi/Repos/n8n-getting-started/unstructured2graph/documents/NSA_Combo_Testing_Functionality_v000_0_1 (1).pdf",
        ),
        DocumentDetails(
            id=2,
            name="Testing Functionality v000_0_2 (1).pdf",
            file_path="/Users/bitovi/Repos/n8n-getting-started/unstructured2graph/documents/Testing_Functionality_v000_0_2 (1).pdf",
        )
    ]

    handle = await client.start_workflow(
        workflow=DocumentIngestion.run,
        args=[document_details],
        id=f"ingest-document-workflow-{uuid.uuid4()}",
        task_queue=DOCUMENT_TASK_QUEUE,
    )

    print("Workflow started with ID:", handle.id)
    document_result = await handle.result()
    print("Workflow result:", document_result)

if __name__ == "__main__":
    asyncio.run(main())