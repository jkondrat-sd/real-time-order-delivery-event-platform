# Event generator

This small script creates realistic order lifecycle events and can either print them (dry-run) or publish them to a Kafka topic (Redpanda) using `kafka-python`.

Example:

```
python services/event_generator/main.py --events 500 --dry-run
python services/event_generator/main.py --events 1000 --topic orders.events --brokers redpanda:29092
```
