import asyncio
from temporalio.client import Client
from temporalio.worker import Worker
from temporalio import workflow

with workflow.unsafe.imports_passed_through():
    from workflows import DocumentIngestion
    from activities import MemgraphActivities
    from shared import DOCUMENT_TASK_QUEUE

async def main():
    client = await Client.connect("localhost:7233")
    activities = MemgraphActivities()   
    document_worker = Worker(
        client,
        task_queue=DOCUMENT_TASK_QUEUE,
        workflows=[DocumentIngestion],
        activities=[
            activities.unstructured_to_graph, 
            activities.clear_working_directory,
            activities.clear_memgraph_db,
            activities.create_index,
            activities.initialize_lightrag,
            activities.afinalize_lightrag
        ]
    )
    print("Worker started.")
    await document_worker.run()

if __name__ == "__main__":
    asyncio.run(main())