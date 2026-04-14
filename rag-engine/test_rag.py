import unittest
import os
import shutil
from rag_engine.rag import RAGEngine, Document

class TestRAGEngine(unittest.TestCase):
    
    @classmethod
    def setUpClass(cls):
        # Create a temporary storage directory for tests
        cls.test_storage = "./test_rag_storage"
        if os.path.exists(cls.test_storage):
            shutil.rmtree(cls.test_storage)
        cls.engine = RAGEngine(persist_directory=cls.test_storage)

    @classmethod
    def tearDownClass(cls):
        # Clean up temporary storage directory after tests
        if os.path.exists(cls.test_storage):
            shutil.rmtree(cls.test_storage)

    def test_ingestion_and_retrieval(self):
        # 1. Ingest test documents
        docs = [
            Document(id="test_1", content="Python is a popular programming language.", metadata={"source": "tech", "topic": "python"}),
            Document(id="test_2", content="Rust is known for memory safety and performance.", metadata={"source": "tech", "topic": "rust"}),
            Document(id="test_3", content="ChromaDB is a vector database for AI applications.", metadata={"source": "db", "topic": "vector_db"})
        ]
        self.engine.ingest_documents(docs)

        # 2. Test retrieval for Python
        results = self.engine.retrieve("What is Python?")
        self.assertTrue(len(results) > 0)
        self.assertIn("Python", results[0].content)
        self.assertEqual(results[0].metadata['topic'], "python")

        # 3. Test retrieval for Rust
        results = self.engine.retrieve("Which language focuses on memory safety?")
        self.assertTrue(len(results) > 0)
        self.assertIn("Rust", results[0].content)
        self.assertEqual(results[0].metadata['topic'], "rust")

    def test_generate_response_no_context(self):
        # Test generation with empty context
        response = self.engine.generate_response("Where is the context?", [])
        self.assertEqual(response, "I'm sorry, I couldn't find any relevant context to answer that question.")

    def test_generate_response_simulated(self):
        # Test simulated generation
        context = [Document(id="test_1", content="Python is great.", metadata={"topic": "python"})]
        response = self.engine.generate_response("Tell me about Python", context)
        self.assertIn("Based on the provided context", response)
        self.assertIn("python", response.lower())

if __name__ == '__main__':
    unittest.main()
