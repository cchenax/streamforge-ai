import json
import logging
from typing import Dict, List, Optional, Any
from .agent import Agent
from .config_loader import ConfigLoader
from .communication import CommunicationManager

class WorkflowEngine:
    def __init__(self, config_path: str):
        self.config_loader = ConfigLoader(config_path)
        self.workflow_config = self.config_loader.load_config()
        self.agents: Dict[str, Agent] = {}
        self.communication_manager = CommunicationManager()
        self.logger = logging.getLogger(__name__)
        
    def initialize_agents(self):
        """Initialize agents based on workflow configuration"""
        for agent_config in self.workflow_config.get('agents', []):
            agent_name = agent_config['name']
            agent_type = agent_config['type']
            agent = Agent.create_agent(agent_type, agent_name, agent_config.get('config', {}))
            self.agents[agent_name] = agent
            self.communication_manager.register_agent(agent_name)
            self.logger.info(f"Initialized agent: {agent_name} (type: {agent_type})")
    
    def execute_workflow(self):
        """Execute the workflow according to the defined steps"""
        self.initialize_agents()
        
        steps = self.workflow_config.get('steps', [])
        for step in steps:
            agent_name = step['agent']
            action = step['action']
            input_data = step.get('input', {})
            
            if agent_name not in self.agents:
                self.logger.error(f"Agent {agent_name} not found")
                continue
            
            agent = self.agents[agent_name]
            try:
                # Check for messages for this agent
                messages = self.communication_manager.get_messages(agent_name)
                if messages:
                    self.logger.info(f"Received {len(messages)} messages for agent {agent_name}")
                    input_data['messages'] = messages
                
                self.logger.info(f"Executing step: {action} on agent {agent_name}")
                result = agent.execute_action(action, input_data)
                self.logger.info(f"Step completed with result: {result}")
                
                # Pass result to next steps if needed
                if 'output_to' in step:
                    output_to = step['output_to']
                    for next_agent, next_input_key in output_to.items():
                        if next_agent in self.agents:
                            # Use communication manager to send message
                            message = {
                                'type': 'result',
                                'key': next_input_key,
                                'data': result
                            }
                            self.communication_manager.send_message(agent_name, next_agent, message)
                            self.logger.info(f"Sent result to agent {next_agent} as {next_input_key}")
            except Exception as e:
                self.logger.error(f"Error executing step: {e}")
                # Handle error according to workflow configuration
                if step.get('on_error', 'continue') == 'stop':
                    self.logger.error("Stopping workflow due to error")
                    return False
        
        self.logger.info("Workflow executed successfully")
        return True
    
    def get_agent_status(self, agent_name: str) -> Optional[Dict[str, Any]]:
        """Get the status of a specific agent"""
        if agent_name in self.agents:
            return self.agents[agent_name].get_status()
        return None
    
    def get_all_agents_status(self) -> Dict[str, Dict[str, Any]]:
        """Get the status of all agents"""
        return {
            agent_name: agent.get_status()
            for agent_name, agent in self.agents.items()
        }
