import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from workflows import DocumentIngestion, QueryProcessing
from activities import process_document, process_query

async def main():
    client = await Client.connect("localhost:7233")
    
    document_worker = Worker(
        client,
        task_queue="my-task-queue",
        workflows=[DocumentIngestion],
        activities=[process_document],
    )
    print("Worker started.")
    await document_worker.run()

    query_worker = Worker(
        client,
        task_queue="my-task-queue",
        workflows=[QueryProcessing],
        activities=[process_query],
    )
    print("Worker started.")
    await query_worker.run()

if __name__ == "__main__":
    asyncio.run(main())