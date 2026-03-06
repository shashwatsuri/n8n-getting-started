import os
import logging

import asyncio
import shutil
from lightrag_memgraph import MemgraphLightRAGWrapper

from memgraph_toolbox.api.memgraph import Memgraph
from unstructured2graph import from_unstructured, create_index
import sources as SOURCES
from dotenv import load_dotenv


load_dotenv()

username = os.getenv("MEMGRAPH_USERNAME", "memgraph")
password = os.getenv("MEMGRAPH_PASSWORD", "memgraph")
url = os.getenv("MEMGRAPH_URI", "localhost")
database = os.getenv("MEMGRAPH_DATABASE", "memgraph")

SCRIPT_DIR = os.path.dirname(os.path.realpath(__file__))
LIGHTRAG_DIR = os.path.join(SCRIPT_DIR, "..", "lightrag_storage.out")


async def from_unstructured_with_prep(sources: list[str] = SOURCES.MEMGRAPH_DOCS_GITHUB_LATEST_RAW):
    # Delete & create LightRAG working directory.
    lightrag_log_file = os.path.join(LIGHTRAG_DIR, "lightrag.log")
    if os.path.exists(lightrag_log_file):
        os.remove(lightrag_log_file)
    if os.path.exists(LIGHTRAG_DIR):
        shutil.rmtree(LIGHTRAG_DIR)
    if not os.path.exists(LIGHTRAG_DIR):
        os.mkdir(LIGHTRAG_DIR)

    # Cleanup Memgraph database.
    memgraph = Memgraph(username=username, password=password, url=url, database=database)
    memgraph.query("MATCH (n) DETACH DELETE n;")
    create_index(memgraph, "Chunk", "hash")

    lightrag_wrapper = MemgraphLightRAGWrapper(
        log_level="WARNING", disable_embeddings=True
    )
    await lightrag_wrapper.initialize(working_dir=LIGHTRAG_DIR)

    await from_unstructured(
        sources,
        memgraph,
        lightrag_wrapper,
        only_chunks=False,
        link_chunks=True,
    )
    await lightrag_wrapper.afinalize()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    asyncio.run(from_unstructured_with_prep(["unstructured2graph/documents/NSA_Combo_Testing_Functionality_v000_0_1 (1).pdf","/Users/bitovi/Repos/n8n-getting-started/unstructured2graph/documents/Testing_Functionality_v000_0_2 (1).pdf"]))
