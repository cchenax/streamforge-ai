# StreamForge RAG Engine

A lightweight, local-first **Retrieval-Augmented Generation (RAG)** engine for real-time AI context retrieval.

## Overview

The RAG Engine allows StreamForge users to:
1.  **Ingest** high-volume streaming data (e.g., from CDC streams) into a vector store.
2.  **Generate** embeddings using local transformer models (e.g., `sentence-transformers`).
3.  **Retrieve** context-relevant document segments for LLM-based queries.
4.  **Augment** LLM prompts with real-time data to reduce hallucinations and improve accuracy.

## Architecture

-   **Vector Store**: Uses `ChromaDB` for persistent local storage and efficient similarity search.
-   **Embedding Model**: Uses `all-MiniLM-L6-v2` (via `sentence-transformers`) for local, fast embedding generation.
-   **LLM Integration**: Supports OpenAI (if an API key is provided) or a template-based simulated response for local development.

## Installation

```bash
pip install -r rag-engine/requirements.txt
```

## Usage

### 1. Initialize the Engine

```python
from rag_engine.rag import RAGEngine, Document

# Initialize with local storage path
engine = RAGEngine(persist_directory="./my_storage")
```

### 2. Ingest Data

You can ingest individual `Document` objects or load them from a JSON file.

```python
# Ingest individual documents
docs = [
    Document(id="1", content="StreamForge AI is awesome.", metadata={"topic": "general", "source": "user_feedback"})
]
engine.ingest_documents(docs)

# Or load from a JSON file
# engine.ingest_from_file("docs.json")
```

### 3. Query and Generate

```python
# Retrieve relevant context
query = "What is StreamForge AI?"
context = engine.retrieve(query, top_k=3)

# Generate a response (uses simulation by default)
response = engine.generate_response(query, context)
print(f"Assistant: {response}")

# Generate with OpenAI (if OPENAI_API_KEY is in .env)
# import os
# response = engine.generate_response(query, context, api_key=os.getenv("OPENAI_API_KEY"))
```

## Running the Demo

To see the RAG engine in action:

```bash
python rag-engine/rag.py
```

This will ingest some sample documents about StreamForge and perform a query.
