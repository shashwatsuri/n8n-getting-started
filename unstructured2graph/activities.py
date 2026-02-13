import asyncio
import os
import shutil
from temporalio import activity
from shared import DocumentDetails,DOCUMENT_TASK_QUEUE
from dotenv import load_dotenv
from memgraph_toolbox.api.memgraph import Memgraph
from lightrag_memgraph import MemgraphLightRAGWrapper
from unstructured2graph import from_unstructured


load_dotenv()

class MemgraphActivities:

    def __init__(self, log_level="WARNING", disable_embeddings=True):
        self.log_level = log_level
        self.disable_embeddings = disable_embeddings
        self.username = os.getenv("MEMGRAPH_USERNAME", "memgraph")
        self.password = os.getenv("MEMGRAPH_PASSWORD", "memgraph")
        self.url = os.getenv("MEMGRAPH_URI", "localhost")
        self.database = os.getenv("MEMGRAPH_DATABASE", "memgraph")
        self.script_dir = os.path.dirname(os.path.realpath(__file__))
        self.lightrag_dir = os.path.join(self.script_dir, "..", "lightrag_storage.out")
        self.memgraph = None
        self.lightrag_wrapper = None

    async def connect_to_memgraph(self) -> Memgraph:
        """Activity to connect to the Memgraph database"""
        self.memgraph = Memgraph(username=self.username, password=self.password, url=self.url, database=self.database)
        return self.memgraph

    @activity.defn
    async def clear_working_directory(self) -> str:
        """Activity to clear the working directory"""
        # Delete & create LightRAG working directory.
        lightrag_log_file = os.path.join(self.lightrag_dir, "lightrag.log")
        if os.path.exists(lightrag_log_file):
            os.remove(lightrag_log_file)
        if os.path.exists(self.lightrag_dir):
            shutil.rmtree(self.lightrag_dir)
        if not os.path.exists(self.lightrag_dir):
            os.mkdir(self.lightrag_dir)
        return "Cleared working directory"

    @activity.defn
    async def clear_memgraph_db(self) -> str:
        """Activity to clear the Memgraph database"""
        # Cleanup Memgraph database.
        if self.memgraph is None:
            self.memgraph = await self.connect_to_memgraph()
        self.memgraph.query("MATCH (n) DETACH DELETE n;")
        return "Cleared Memgraph database"

    @activity.defn
    async def create_index(self, label: str, property: str) -> str:
        """Activity to create an index in Memgraph"""
        if self.memgraph is None:
            self.memgraph = await self.connect_to_memgraph()
        self.memgraph.query(f"CREATE INDEX ON :{label}({property});")
        return f"Created index on :{label}({property})"


    # @activity.defn
    # async def process_query(self, query_details: QueryDetails) -> str:
    #     """Activity to process a query and return a result"""
    #     if self.memgraph is None:
    #         self.memgraph = await self.connect_to_memgraph()
    #     self.memgraph.query(query_details.query)
    #     # Here you would add your actual query processing logic
    #     # For this example, we'll just return a success message
    #     return f"Processed query: {query_details.name}"
    
    @activity.defn
    async def initialize_lightrag(self) -> str:
        """Activity to initialize the MemgraphLightRAGWrapper"""
        self.lightrag_wrapper = MemgraphLightRAGWrapper(
            log_level=self.log_level, disable_embeddings=self.disable_embeddings
        )
        await self.lightrag_wrapper.initialize(working_dir=self.lightrag_dir)
        return "Initialized MemgraphLightRAGWrapper"
    
    @activity.defn
    async def unstructured_to_graph(self, sources: list[str], only_chunks: bool, link_chunks: bool) -> str:
        """Activity to convert unstructured data to a graph"""
        await from_unstructured(
            sources,
            self.memgraph,
            self.lightrag_wrapper,
            only_chunks=only_chunks,
            link_chunks=link_chunks,
        )
        return "Converted unstructured data to graph"
    
    @activity.defn
    async def afinalize_lightrag(self) -> str:
        """Activity to finalize the LightRAG wrapper"""
        await self.lightrag_wrapper.afinalize()
        return "Finalized MemgraphLightRAGWrapper"