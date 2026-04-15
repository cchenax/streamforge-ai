import os
import json
import logging
from dataclasses import dataclass, asdict
from typing import List, Dict, Optional
from pathlib import Path
from dotenv import load_dotenv

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("RAG-Engine")

load_dotenv()

@dataclass
class Document:
    """Represents a source document for RAG."""
    id: str
    content: str
    metadata: Dict[str, str]

    def to_dict(self):
        return asdict(self)

class RAGEngine:
    """
    A robust RAG (Retrieval-Augmented Generation) engine for StreamForge.
    
    Supports ChromaDB for vector storage and local/remote embeddings and LLMs.
    """
    
    def __init__(self, collection_name: str = "streamforge_docs", persist_directory: str = "./rag_storage"):
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        self.is_mock = False
        
        try:
            import chromadb
            from chromadb.utils import embedding_functions
            
            self.client = chromadb.PersistentClient(path=persist_directory)
            self.embedding_fn = embedding_functions.SentenceTransformerEmbeddingFunction(
                model_name="all-MiniLM-L6-v2"
            )
            self.collection = self.client.get_or_create_collection(
                name=collection_name, 
                embedding_function=self.embedding_fn
            )
            logger.info(f"Initialized ChromaDB at {persist_directory} with collection '{collection_name}'")
        except (ImportError, Exception) as e:
            logger.warning(f"Using Mock RAG Engine because: {e}")
            self.is_mock = True
            self.mock_db = []

    def ingest_documents(self, documents: List[Document]):
        """
        Add documents to the vector store.
        """
        if not documents:
            logger.warning("No documents to ingest.")
            return

        if self.is_mock:
            for doc in documents:
                self.mock_db.append(doc)
            logger.info(f"Mock Ingested {len(documents)} documents.")
            return

        ids = [doc.id for doc in documents]
        contents = [doc.content for doc in documents]
        metadatas = [doc.metadata for doc in documents]
        
        self.collection.add(
            ids=ids,
            documents=contents,
            metadatas=metadatas
        )
        logger.info(f"Ingested {len(documents)} documents into ChromaDB.")

    def ingest_from_file(self, file_path: str):
        """
        Load documents from a JSON file.
        Format: [{"id": "...", "content": "...", "metadata": {...}}, ...]
        """
        path = Path(file_path)
        if not path.exists():
            logger.error(f"File {file_path} not found.")
            return

        try:
            with open(path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                docs = [Document(**item) for item in data]
                self.ingest_documents(docs)
        except Exception as e:
            logger.error(f"Error loading documents from {file_path}: {e}")

    def retrieve(self, query: str, top_k: int = 3) -> List[Document]:
        """
        Retrieve top-k relevant documents for the query.
        """
        if self.is_mock:
            # Simple keyword search for the mock
            results = []
            for doc in self.mock_db:
                if any(word.lower() in doc.content.lower() for word in query.split()):
                    results.append(doc)
            return results[:top_k]

        results = self.collection.query(
            query_texts=[query],
            n_results=top_k
        )
        
        docs = []
        if results['ids'] and results['ids'][0]:
            for i in range(len(results['ids'][0])):
                docs.append(Document(
                    id=results['ids'][0][i],
                    content=results['documents'][0][i],
                    metadata=results['metadatas'][0][i]
                ))
        return docs

    def generate_response(self, query: str, context: List[Document], api_key: Optional[str] = None) -> str:
        """
        Generate a response based on retrieved context using an LLM.
        If api_key is provided, uses OpenAI. Otherwise, uses a template-based simulation.
        """
        if not context:
            return "I'm sorry, I couldn't find any relevant context to answer that question."
        
        context_str = "\n---\n".join([f"Source: {doc.metadata.get('source', 'unknown')}\nContent: {doc.content}" for doc in context])
        
        prompt = (
            f"You are a helpful assistant for StreamForge AI. Use the following context to answer the user's question.\n"
            f"If you don't know the answer, just say you don't know, don't try to make up an answer.\n\n"
            f"CONTEXT:\n{context_str}\n\n"
            f"USER QUESTION: {query}\n\n"
            f"HELPFUL ANSWER:"
        )
        
        if api_key:
            try:
                from openai import OpenAI
                client = OpenAI(api_key=api_key)
                response = client.chat.completions.create(
                    model="gpt-3.5-turbo",
                    messages=[
                        {"role": "system", "content": "You are a helpful technical assistant for StreamForge."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.2
                )
                return response.choices[0].message.content
            except Exception as e:
                logger.error(f"Error calling LLM API: {e}")
                return f"[ERROR] Could not generate response via LLM API: {e}"
        
        # Enhanced Simulation
        simulated_response = (
            f"Based on the provided context, I found information relevant to your query: '{query}'.\n"
            f"The documents mention details about: " + ", ".join([doc.metadata.get('topic', 'general') for doc in context]) + ".\n"
            f"Key information from context: {context[0].content[:100]}..."
        )
        return simulated_response

def run_demo():
    """
    Run a full RAG cycle demo.
    """
    engine = RAGEngine()
    
    # 1. Build a demo manifest of documents
    demo_docs = [
        Document(
            id="doc_1",
            content="StreamForge AI is an open-source platform for real-time data ingestion using CDC (Change Data Capture). It leverages Flink for stream processing.",
            metadata={"source": "overview", "topic": "cdc"}
        ),
        Document(
            id="doc_2",
            content="The prefetch engine in StreamForge optimizes ML workloads by caching hot objects in memory before they are requested, reducing latency by up to 50%.",
            metadata={"source": "prefetch_manual", "topic": "caching"}
        ),
        Document(
            id="doc_3",
            content="Retrieval-Augmented Generation (RAG) combines search with LLMs to provide context-aware answers. StreamForge uses ChromaDB for its RAG engine.",
            metadata={"source": "rag_doc", "topic": "llm"}
        )
    ]
    
    logger.info("Step 1: Ingesting documents...")
    engine.ingest_documents(demo_docs)
    
    # 2. Perform a query
    query = "How does StreamForge optimize ML workloads?"
    logger.info(f"Step 2: Querying: '{query}'")
    
    context = engine.retrieve(query)
    logger.info(f"Step 3: Retrieved {len(context)} relevant documents.")
    for i, doc in enumerate(context):
        logger.info(f"  {i+1}. Source: {doc.metadata['source']} - {doc.content[:50]}...")
        
    # 3. Generate a response
    logger.info("Step 4: Generating response...")
    response = engine.generate_response(query, context)
    print("\n" + "="*50)
    print(response)
    print("="*50 + "\n")

if __name__ == "__main__":
    run_demo()
