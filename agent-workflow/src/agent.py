from abc import ABC, abstractmethod
from typing import Dict, Any, Optional
import logging
from .integration import IntegrationManager

class Agent(ABC):
    def __init__(self, name: str, config: Dict[str, Any]):
        self.name = name
        self.config = config
        self.inputs: Dict[str, Any] = {}
        self.outputs: Dict[str, Any] = {}
        self.status = 'ready'
        self.logger = logging.getLogger(__name__)
    
    @abstractmethod
    def execute_action(self, action: str, input_data: Dict[str, Any]) -> Any:
        """Execute an action on the agent"""
        pass
    
    def set_input(self, key: str, value: Any):
        """Set input data for the agent"""
        self.inputs[key] = value
        self.logger.info(f"Set input {key} for agent {self.name}")
    
    def get_output(self, key: str) -> Optional[Any]:
        """Get output data from the agent"""
        return self.outputs.get(key)
    
    def get_status(self) -> Dict[str, Any]:
        """Get the status of the agent"""
        return {
            'name': self.name,
            'status': self.status,
            'inputs': self.inputs,
            'outputs': self.outputs
        }
    
    @classmethod
    def create_agent(cls, agent_type: str, name: str, config: Dict[str, Any]) -> 'Agent':
        """Create an agent based on type"""
        if agent_type == 'data_ingestion':
            return DataIngestionAgent(name, config)
        elif agent_type == 'stream_processor':
            return StreamProcessorAgent(name, config)
        elif agent_type == 'storage_sink':
            return StorageSinkAgent(name, config)
        elif agent_type == 'prefetch':
            return PrefetchAgent(name, config)
        else:
            raise ValueError(f"Unknown agent type: {agent_type}")

class DataIngestionAgent(Agent):
    def execute_action(self, action: str, input_data: Dict[str, Any]) -> Any:
        if action == 'start_ingestion':
            self.status = 'ingesting'
            # Simulate data ingestion
            self.logger.info(f"Starting data ingestion with config: {self.config}")
            result = {'status': 'ingestion_started', 'source': self.config.get('source', 'mysql')}
            self.outputs['ingestion_result'] = result
            self.status = 'completed'
            return result
        else:
            raise ValueError(f"Unknown action: {action}")

class StreamProcessorAgent(Agent):
    def execute_action(self, action: str, input_data: Dict[str, Any]) -> Any:
        if action == 'process_stream':
            self.status = 'processing'
            # Simulate stream processing
            self.logger.info(f"Processing stream with config: {self.config}")
            result = {'status': 'processing_completed', 'processed_records': 1000}
            self.outputs['processing_result'] = result
            self.status = 'completed'
            return result
        else:
            raise ValueError(f"Unknown action: {action}")

class StorageSinkAgent(Agent):
    def execute_action(self, action: str, input_data: Dict[str, Any]) -> Any:
        if action == 'write_to_storage':
            self.status = 'writing'
            # Simulate writing to storage
            self.logger.info(f"Writing to storage with config: {self.config}")
            result = {'status': 'write_completed', 'storage': self.config.get('storage', 'minio')}
            self.outputs['write_result'] = result
            self.status = 'completed'
            return result
        else:
            raise ValueError(f"Unknown action: {action}")

class PrefetchAgent(Agent):
    def __init__(self, name: str, config: Dict[str, Any]):
        super().__init__(name, config)
        self.integration_manager = IntegrationManager()
    
    def execute_action(self, action: str, input_data: Dict[str, Any]) -> Any:
        if action == 'prefetch_data':
            self.status = 'prefetching'
            self.logger.info(f"Prefetching data with config: {self.config}")
            result = self.integration_manager.run_prefetch_demo(self.config)
            self.outputs['prefetch_result'] = result
            self.status = 'completed'
            return result
        else:
            raise ValueError(f"Unknown action: {action}")
