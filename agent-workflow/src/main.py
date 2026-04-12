import sys
import logging
from workflow_engine import WorkflowEngine

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

def main():
    if len(sys.argv) != 2:
        print("Usage: python main.py <config_file>")
        sys.exit(1)
    
    config_path = sys.argv[1]
    
    try:
        engine = WorkflowEngine(config_path)
        success = engine.execute_workflow()
        
        if success:
            print("Workflow executed successfully")
            # Print agent statuses
            statuses = engine.get_all_agents_status()
            print("\nAgent Statuses:")
            for agent_name, status in statuses.items():
                print(f"\n{agent_name}:")
                print(f"  Status: {status['status']}")
                print(f"  Inputs: {status['inputs']}")
                print(f"  Outputs: {status['outputs']}")
        else:
            print("Workflow execution failed")
            sys.exit(1)
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
