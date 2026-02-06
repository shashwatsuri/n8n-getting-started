import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from workflows import DocumentIngestion, QueryProcessing
    from activities import process_document, process_query
    from shared import DOCUMENT_TASK_QUEUE, QUERY_TASK_QUEUE

async def main():
    client = await Client.connect("localhost:7233")
    
    document_worker = Worker(
        client,
        task_queue=DOCUMENT_TASK_QUEUE,
        workflows=[DocumentIngestion],
        activities=[process_document],
    )
    print("Worker started.")
    await document_worker.run()

    query_worker = Worker(
        client,
        task_queue=QUERY_TASK_QUEUE,
        workflows=[QueryProcessing],
        activities=[process_query],
    )
    print("Worker started.")
    await query_worker.run()

if __name__ == "__main__":
    asyncio.run(main())