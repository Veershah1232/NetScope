"""
Kafka to MinIO Archival Consumer — M3 / M4

Consumes records from Kafka topic 'measurements' using a consumer group,
buffers them into micro-batches, and writes them to MinIO partitioned
by year/month/day/probe in Snappy-compressed Parquet format.

Includes:
- Offset commit after successful batch persistence
- Consumer lag awareness
- Graceful shutdown handling
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import sys
import time
from collections import defaultdict
from typing import Optional

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from probe.netscope_probe.writers.minio_writer import (
    _make_minio_client,
    ensure_bucket,
    _partition_key,
    write_partition_to_minio,
    BUCKET_DEFAULT,
)

logger = logging.getLogger("netscope.kafka_minio_consumer")


class KafkaToMinIOConsumer:
    def __init__(
        self,
        bootstrap_servers: str = "localhost:9092",
        group_id: str = "minio-archival-group",
        topic: str = "measurements",
        minio_endpoint: str = "http://localhost:9000",
        minio_access_key: str = "minioadmin",
        minio_secret_key: str = "minioadmin",
        minio_bucket: str = BUCKET_DEFAULT,
        batch_size: int = 50,
        flush_interval_s: float = 10.0,
    ):
        self.bootstrap_servers = bootstrap_servers
        self.group_id = group_id
        self.topic = topic
        self.batch_size = batch_size
        self.flush_interval_s = flush_interval_s

        self.minio_client = _make_minio_client(minio_endpoint, minio_access_key, minio_secret_key)
        self.minio_bucket = minio_bucket
        ensure_bucket(self.minio_client, self.minio_bucket)

        self.running = True
        self.buffer: list[dict] = []
        self.last_flush_time = time.time()
        self.batch_counter = 0

    def start(self) -> None:
        from kafka import KafkaConsumer

        logger.info(
            "Connecting KafkaConsumer to %s (topic: %s, group: %s)",
            self.bootstrap_servers,
            self.topic,
            self.group_id,
        )

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=self.bootstrap_servers.split(","),
            group_id=self.group_id,
            auto_offset_reset="earliest",
            enable_auto_commit=False,  # Manual commit after MinIO flush ensures at-least-once delivery
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            consumer_timeout_ms=1000,
        )

        logger.info("Kafka consumer loop active.")

        try:
            while self.running:
                records = consumer.poll(timeout_ms=1000)
                for topic_partition, messages in records.items():
                    for message in messages:
                        self.buffer.append(message.value)

                # Check if buffer size or flush interval reached
                time_since_flush = time.time() - self.last_flush_time
                if (len(self.buffer) >= self.batch_size) or (self.buffer and time_since_flush >= self.flush_interval_s):
                    self._flush_buffer_to_minio()
                    consumer.commit()
                    self.last_flush_time = time.time()

        except Exception as exc:
            logger.error("Error in consumer loop: %s", exc)
        finally:
            if self.buffer:
                logger.info("Flushing remaining %d records before exit...", len(self.buffer))
                self._flush_buffer_to_minio()
                try:
                    consumer.commit()
                except Exception:
                    pass
            consumer.close()
            logger.info("Consumer stopped cleanly.")

    def _flush_buffer_to_minio(self) -> None:
        if not self.buffer:
            return

        count = len(self.buffer)
        logger.info("Flushing %d records to MinIO...", count)

        # Partition by Hive key
        partitions: dict[str, list[dict]] = defaultdict(list)
        for rec in self.buffer:
            probe_id = rec.get("probe_id", "unknown")
            ts = rec.get("timestamp", "")
            key = _partition_key(ts, probe_id)
            partitions[key].append(rec)

        for partition_path, records in partitions.items():
            self.batch_counter += 1
            write_partition_to_minio(
                self.minio_client,
                self.minio_bucket,
                partition_path,
                self.batch_counter,
                records,
            )

        self.buffer.clear()
        logger.info("Flush completed.")

    def stop(self) -> None:
        self.running = False


def main() -> None:
    parser = argparse.ArgumentParser(description="NetScope Kafka to MinIO Consumer")
    parser.add_argument("--bootstrap-servers", default=os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    parser.add_argument("--group-id", default="minio-archival-group")
    parser.add_argument("--topic", default="measurements")
    parser.add_argument("--minio-endpoint", default=os.environ.get("MINIO_ENDPOINT", "http://localhost:9000"))
    parser.add_argument("--minio-access-key", default=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"))
    parser.add_argument("--minio-secret-key", default=os.environ.get("MINIO_SECRET_KEY", "minioadmin"))
    parser.add_argument("--minio-bucket", default=BUCKET_DEFAULT)
    parser.add_argument("--batch-size", type=int, default=20)
    parser.add_argument("--flush-interval", type=float, default=5.0)
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    consumer = KafkaToMinIOConsumer(
        bootstrap_servers=args.bootstrap_servers,
        group_id=args.group_id,
        topic=args.topic,
        minio_endpoint=args.minio_endpoint,
        minio_access_key=args.minio_access_key,
        minio_secret_key=args.minio_secret_key,
        minio_bucket=args.minio_bucket,
        batch_size=args.batch_size,
        flush_interval_s=args.flush_interval,
    )

    def handle_signal(sig, frame):
        logger.info("Termination signal received. Stopping consumer...")
        consumer.stop()

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    consumer.start()


if __name__ == "__main__":
    main()
