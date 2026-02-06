import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from workflows import DocumentIngestion
from activities import process_document

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

    query_worker

if __name__ == "__main__":
    asyncio.run(main())