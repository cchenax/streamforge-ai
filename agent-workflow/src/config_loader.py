import json
import os
from typing import Dict, Any
import logging

class ConfigLoader:
    def __init__(self, config_path: str):
        self.config_path = config_path
        self.logger = logging.getLogger(__name__)
    
    def load_config(self) -> Dict[str, Any]:
        """Load workflow configuration from file"""
        if not os.path.exists(self.config_path):
            self.logger.error(f"Config file not found: {self.config_path}")
            raise FileNotFoundError(f"Config file not found: {self.config_path}")
        
        try:
            with open(self.config_path, 'r') as f:
                config = json.load(f)
            self.logger.info(f"Loaded config from {self.config_path}")
            return config
        except json.JSONDecodeError as e:
            self.logger.error(f"Error parsing config file: {e}")
            raise
        except Exception as e:
            self.logger.error(f"Error loading config file: {e}")
            raise
