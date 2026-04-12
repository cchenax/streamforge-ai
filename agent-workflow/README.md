# Multi-Agent Collaboration Workflow

## Overview

The Multi-Agent Collaboration Workflow module is designed to orchestrate and manage multiple agents working together in a data pipeline. It provides a flexible framework for defining and executing workflows that involve different types of agents, such as data ingestion, stream processing, storage sinking, and data prefetching.

## Features

- **Workflow Orchestration**: Define and execute multi-step workflows with clear dependencies
- **Agent Communication**: Enables message passing between agents
- **Integration with Existing Components**: Seamlessly integrates with StreamForge AI's existing components like prefetch-engine
- **Configurable Agents**: Support for different types of agents with customizable configurations
- **Error Handling**: Robust error handling with configurable error policies

## Directory Structure

```
agent-workflow/
├── config/              # Workflow configuration files
├── examples/            # Example workflows
├── src/                 # Source code
│   ├── agent.py         # Agent base class and implementations
│   ├── communication.py # Agent communication manager
│   ├── config_loader.py # Configuration loader
│   ├── integration.py   # Integration with existing components
│   ├── main.py          # Main entry point
│   └── workflow_engine.py # Workflow engine
└── README.md            # This file
```

## Getting Started

### Prerequisites

- Python 3.7+
- Dependencies from prefetch-engine (if using prefetch agent)

### Installation

1. Ensure you have the necessary dependencies installed:

```bash
# Install prefetch-engine dependencies
cd ../prefetch-engine
pip install -r requirements.txt
```

### Running a Workflow

1. Create a workflow configuration file (see `config/example_workflow.json` for reference)

2. Run the workflow using the main script:

```bash
cd agent-workflow/src
python main.py ../config/example_workflow.json
```

## Agent Types

### 1. Data Ingestion Agent

Responsible for ingesting data from operational databases using CDC (Change Data Capture).

**Actions**: `start_ingestion`

**Configuration**: 
- `source`: Data source (e.g., "mysql")
- `connection_string`: Database connection string

### 2. Stream Processor Agent

Processes streaming data for feature generation and transformation.

**Actions**: `process_stream`

**Configuration**:
- `framework`: Streaming framework (e.g., "flink")
- `parallelism`: Number of parallel processing units

### 3. Storage Sink Agent

Writes processed data to storage systems like MinIO/S3.

**Actions**: `write_to_storage`

**Configuration**:
- `storage`: Storage system (e.g., "minio")
- `bucket`: Storage bucket name
- `prefix`: Object key prefix

### 4. Prefetch Agent

Analyzes access patterns and prefetches data into cache for ML workloads.

**Actions**: `prefetch_data`

**Configuration**:
- `demo_dir`: Base directory for demo files
- `top_n`: Number of hot files to prefetch
- `job_id`: Unique job identifier

## Workflow Configuration

A workflow configuration is a JSON file that defines:

1. **Agents**: List of agents participating in the workflow
2. **Steps**: Sequence of actions to execute, including:
   - `agent`: Name of the agent to execute
   - `action`: Action to perform
   - `input`: Input data for the action
   - `output_to`: Mapping of next agents and input keys
   - `on_error`: Error handling policy ("continue" or "stop")

### Example Workflow

See `config/example_workflow.json` for a complete example that demonstrates a data pipeline workflow with all agent types.

## Integration with Existing Components

The module integrates with StreamForge AI's existing components:

- **prefetch-engine**: The Prefetch Agent uses the prefetch-engine to simulate data prefetching and ML job execution

## Extending the Framework

To add a new agent type:

1. Create a new class that inherits from `Agent`
2. Implement the `execute_action` method
3. Add the agent type to the `create_agent` class method in the `Agent` class

## Logging

The framework uses Python's standard logging module. Logs are configured to show info-level messages by default.

## Future Enhancements

- Support for more agent types
- Real-time workflow monitoring
- Visual workflow editor
- Integration with more StreamForge AI components
- Distributed execution support
