from dataclasses import dataclass

DOCUMENT_TASK_QUEUE = "document-processing-queue"
QUERY_TASK_QUEUE = "query-processing-queue"

# from enum import Enum

# class DocumentState(Enum):
#     PENDING = "pending"
#     PROCESSING = "processing"
#     COMPLETED = "completed"
#     FAILED = "failed"

# class QueryState(Enum):
#     PENDING = "pending"
#     PROCESSING = "processing"
#     COMPLETED = "completed"
#     FAILED = "failed"

@dataclass
class DocumentDetails:
    id: int
    name: str
    file_path: str
    # metadata: dict
    # state: DocumentState

@dataclass
class QueryDetails:
    id: int
    name: str
    # metadata: dict
    # state: QueryState