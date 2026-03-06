from datetime import timedelta

from temporalio import workflow
from temporalio.common import RetryPolicy
from temporalio.exceptions import ActivityError

with workflow.unsafe.imports_passed_through():
    from shared import DocumentDetails
   

@workflow.defn
class DocumentIngestion:
    retry_policy = RetryPolicy(
        maximum_attempts=3,
        maximum_interval=timedelta(seconds=10),
        non_retryable_error_types=["FileNotFoundError", "PermissionError"],
    )
    @workflow.run
    async def run(self, document_details: list[DocumentDetails]) -> str:
        print(await workflow.execute_activity(
            "clear_working_directory",
            schedule_to_close_timeout=timedelta(seconds=10),
        ))
        print(await workflow.execute_activity(
            "clear_memgraph_db",
            schedule_to_close_timeout=timedelta(seconds=10),
        ))
        print(await workflow.execute_activity(
            "create_index",
            args=["Chunk", "hash"],
            schedule_to_close_timeout=timedelta(seconds=10),
        ))
        print(await workflow.execute_activity(
            "initialize_lightrag",
            schedule_to_close_timeout=timedelta(seconds=10)
            ))
        
        sources = [doc.file_path for doc in document_details]

        print(await workflow.execute_activity(
            "unstructured_to_graph",
            args=[sources, False, True], 
            schedule_to_close_timeout=timedelta(seconds=100000),
            retry_policy=self.retry_policy
            ))
        
        print(await workflow.execute_activity(
            "afinalize_lightrag",
            schedule_to_close_timeout=timedelta(seconds=1000)
            ))
        document_names = ", ".join([doc.name for doc in document_details])
        return f"Ingested documents: {document_names}"

