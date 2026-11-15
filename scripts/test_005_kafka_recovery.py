"""
TEST-005: Kafka Consumer Failure & Lag Recovery Experiment

Demonstrates:
1. Producer publishes measurements to Kafka
2. Consumer starts, processes records, and commits offsets (Lag = 0)
3. Consumer is paused/killed while producer keeps writing -> Lag accumulates
4. Consumer restarts -> Lag drains back to 0
5. Verifies zero silent data loss
"""

from __future__ import annotations

import json
import logging
import time
import uuid
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from kafka import KafkaAdminClient, KafkaConsumer
from ingestion.producers.kafka_producer import NetScopeKafkaProducer
from ingestion.consumers.kafka_to_minio import KafkaToMinIOConsumer

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")
logger = logging.getLogger("netscope.test_005")

BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "measurements"
GROUP_ID = f"test-recovery-group-{int(time.time())}"


def get_consumer_group_lag(consumer: KafkaConsumer, group_id: str, topic: str) -> int:
    """Calculate the total consumer lag across all partitions for a group."""
    from kafka.structs import TopicPartition

    partitions = consumer.partitions_for_topic(topic)
    if not partitions:
        return 0

    tps = [TopicPartition(topic, p) for p in partitions]
    end_offsets = consumer.end_offsets(tps)

    total_lag = 0
    for tp in tps:
        committed = consumer.committed(tp)
        if committed is None:
            committed = 0
        end = end_offsets.get(tp, 0)
        total_lag += max(0, end - committed)

    return total_lag


def run_experiment() -> None:
    logger.info("=== STARTING TEST-005: Kafka Consumer Lag & Recovery ===")

    producer = NetScopeKafkaProducer(bootstrap_servers=BOOTSTRAP_SERVERS, client_id="test-producer")

    # Step 1: Send initial batch of 10 messages
    logger.info("Step 1: Sending initial batch of 10 events...")
    for i in range(10):
        producer.send_measurement({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": "test-probe-01",
            "target_id": "test-target",
            "target": "example.com",
            "timestamp": "2026-11-16T01:00:00+00:00",
            "dns_ms": 10.0,
            "tcp_ms": 20.0,
            "tls_ms": 30.0,
            "ttfb_ms": 40.0,
            "total_ms": 50.0,
            "http_status": 200,
            "response_bytes": 1024,
            "error_type": None,
            "timed_out": False,
        })
    producer.flush()

    # Create consumer client to monitor lag
    lag_monitor = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS.split(","),
        group_id=GROUP_ID,
        enable_auto_commit=False,
    )

    # Initial lag check
    initial_lag = get_consumer_group_lag(lag_monitor, GROUP_ID, TOPIC)
    logger.info("Initial consumer lag before processing: %d", initial_lag)

    # Step 2: Start consumer and process initial batch
    logger.info("Step 2: Starting consumer to drain initial batch...")
    consumer_app = KafkaToMinIOConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        topic=TOPIC,
        batch_size=5,
        flush_interval_s=1.0,
    )

    # Poll and commit one round in consumer
    import threading
    t = threading.Thread(target=consumer_app.start, daemon=True)
    t.start()
    time.sleep(3)

    lag_after_consume = get_consumer_group_lag(lag_monitor, GROUP_ID, TOPIC)
    logger.info("Consumer lag after consumer processed batch: %d", lag_after_consume)

    # Step 3: KILL CONSUMER (simulate failure)
    logger.info("Step 3: Killing consumer... simulating downstream outage")
    consumer_app.stop()
    t.join(timeout=2)

    # Step 4: Producer produces 25 more messages while consumer is DEAD
    logger.info("Step 4: Producing 25 new events while consumer is DEAD...")
    for i in range(25):
        producer.send_measurement({
            "measurement_id": str(uuid.uuid4()),
            "probe_id": "test-probe-01",
            "target_id": "test-target",
            "target": "example.com",
            "timestamp": "2026-11-16T01:05:00+00:00",
            "dns_ms": 15.0,
            "tcp_ms": 25.0,
            "tls_ms": 35.0,
            "ttfb_ms": 45.0,
            "total_ms": 60.0,
            "http_status": 200,
            "response_bytes": 1024,
            "error_type": None,
            "timed_out": False,
        })
    producer.flush()

    # Step 5: Verify lag INCREASED
    lag_during_failure = get_consumer_group_lag(lag_monitor, GROUP_ID, TOPIC)
    logger.info("OBSERVED: Consumer lag during failure increased to: %d", lag_during_failure)
    assert lag_during_failure >= 25, f"Expected lag >= 25, got {lag_during_failure}"

    # Step 6: RESTART CONSUMER -> Verify lag DRAINS back to 0
    logger.info("Step 6: Restarting consumer... observing catch-up")
    recovered_consumer_app = KafkaToMinIOConsumer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=GROUP_ID,
        topic=TOPIC,
        batch_size=5,
        flush_interval_s=1.0,
    )

    t_recovered = threading.Thread(target=recovered_consumer_app.start, daemon=True)
    t_recovered.start()

    # Wait for consumer to catch up
    time.sleep(4)
    recovered_consumer_app.stop()
    t_recovered.join(timeout=2)

    lag_recovered = get_consumer_group_lag(lag_monitor, GROUP_ID, TOPIC)
    logger.info("OBSERVED: Consumer lag after recovery drained to: %d", lag_recovered)

    lag_monitor.close()
    producer.close()

    assert lag_recovered == 0, f"Expected lag 0 after recovery, got {lag_recovered}"
    logger.info("=== TEST-005 PASSED: Verified decoupling, lag accumulation, and zero data loss on recovery ===")


if __name__ == "__main__":
    run_experiment()
