from dataclasses import dataclass

DOCUMENT_TASK_QUEUE = "document-processing-queue"

@dataclass
class DocumentDetails:
    id: int
    name: str
    file_path: str
    # metadata: dict
    # state: DocumentState