from typing import Dict, Any, List
import logging

class CommunicationManager:
    def __init__(self):
        self.message_queues: Dict[str, List[Dict[str, Any]]] = {}
        self.logger = logging.getLogger(__name__)
    
    def register_agent(self, agent_name: str):
        """Register an agent with the communication manager"""
        if agent_name not in self.message_queues:
            self.message_queues[agent_name] = []
            self.logger.info(f"Registered agent {agent_name} in communication manager")
    
    def send_message(self, sender: str, receiver: str, message: Dict[str, Any]):
        """Send a message from one agent to another"""
        if receiver not in self.message_queues:
            self.logger.error(f"Receiver {receiver} not registered")
            return False
        
        message_with_metadata = {
            'sender': sender,
            'receiver': receiver,
            'message': message,
            'timestamp': self._get_timestamp()
        }
        
        self.message_queues[receiver].append(message_with_metadata)
        self.logger.info(f"Message sent from {sender} to {receiver}")
        return True
    
    def get_messages(self, agent_name: str) -> List[Dict[str, Any]]:
        """Get all messages for an agent"""
        if agent_name not in self.message_queues:
            self.logger.error(f"Agent {agent_name} not registered")
            return []
        
        messages = self.message_queues[agent_name].copy()
        # Clear the queue after retrieving messages
        self.message_queues[agent_name] = []
        self.logger.info(f"Retrieved {len(messages)} messages for agent {agent_name}")
        return messages
    
    def _get_timestamp(self) -> str:
        """Get current timestamp"""
        import datetime
        return datetime.datetime.now().isoformat()
