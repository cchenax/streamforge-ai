# Stream Processor (Flink)

This module provides a basic Flink job that:
- reads Debezium CDC events from Kafka,
- counts events per user in tumbling time windows,
- emits processed feature JSON to Kafka.

Main class:
- `ai.streamforge.processor.CdcUserEventCountJob`

Input topic (default):
- `streamforge.streamforge.customers`

Output topic (default):
- `streamforge.features.user_event_counts`
