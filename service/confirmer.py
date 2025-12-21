import logging
from typing import List, Dict, Any
from dataclasses import dataclass
from src.database.graph import DAG
from src.database.redis import Redis
from src.database.queues import RabbitMQ
from src.database.influxdb import InfluxDB
import os
import asyncio
import json
import random

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Config:
    WEIGHT: int = 10

class ConfirmationProcessor:
    def __init__(self, redis: Redis, dag: DAG, influx: InfluxDB):
        self.dag = dag
        self.redis = redis
        self.influx = influx
        self.config = Config()

    async def check_pool(self) -> None:
        """Check the pool of unconfirmed transactions and process those above the threshold."""
        try:
            # Fetch pre-calculated weights from the DAG API
            weights_response = await self.dag.get_weights()
            if not weights_response or weights_response.get("status") != "success":
                logger.error(f"Failed to fetch weights: {weights_response.get('message') if weights_response else 'No response'}")
                return

            # Extract nodes and filter those exceeding the threshold
            nodes: List[Dict[str, Any]] = weights_response.get("nodes", [])
            confirmed_nodes = [node for node in nodes if node["weight"] >= self.config.WEIGHT]
            if not confirmed_nodes:
                logger.debug("No nodes above threshold found")
                return

            # Record confirmed nodes in Redis in a batch
            success = await self.record_in_redis(confirmed_nodes)
            if not success:
                logger.error("Failed to record confirmed nodes in Redis")
                return

            # Remove confirmed nodes from the DAG in a batch
            await self.remove_from_dag([node["hash"] for node in confirmed_nodes])

        except Exception as e:
            logger.error(f"Error in check_pool: {e}", exc_info=True)
            await self.influx.write_metric(
                measurement="check_pool_error",
                tags={"service": "confirmer"},
                fields={"count": 1}
            )

    async def record_in_redis(self, nodes: List[Dict[str, Any]]) -> bool:
        """Record confirmed nodes in Redis in a batch."""
        try:
            async with self.redis.client.pipeline() as pipe:
                for node in nodes:
                    confirmed_key = f"confirmed:{node['hash']}"
                    # Store the entire node data (hash, weight, data) as JSON
                    await pipe.set(confirmed_key, json.dumps(node["data"]))
                await pipe.execute()
            logger.info(f"Recorded {len(nodes)} confirmed nodes in Redis")
            await self.influx.write_metric(
                measurement="nodes_recorded",
                tags={"service": "confirmer"},
                fields={"count": len(nodes)}
            )
            return True
        except Exception as e:
            logger.error(f"Failed to record nodes in Redis: {e}", exc_info=True)
            await self.influx.write_metric(
                measurement="redis_record_error",
                tags={"service": "confirmer"},
                fields={"count": 1}
            )
            return False

    async def remove_from_dag(self, hashes: List[str]) -> None:
        """Remove nodes from the DAG in a batch."""
        try:
            response = await self.dag.remove_nodes(hashes)
            if response and response.get("status") == "success":
                logger.info(f"Successfully removed {len(hashes)} nodes from DAG")
                await self.influx.write_metric(
                    measurement="nodes_removed",
                    tags={"service": "confirmer"},
                    fields={"count": len(hashes)}
                )
            else:
                logger.error(f"Failed to remove nodes: {response.get('message') if response else 'No response'}")
                await self.influx.write_metric(
                    measurement="remove_nodes_error",
                    tags={"service": "confirmer"},
                    fields={"count": 1}
                )
        except Exception as e:
            logger.error(f"Error removing nodes from DAG: {e}", exc_info=True)
            await self.influx.write_metric(
                measurement="dag_remove_error",
                tags={"service": "confirmer"},
                fields={"count": 1}
            )

    async def periodic_check_pool(self, stop_event=None):
        while not (stop_event and stop_event.is_set()):
            await self.check_pool()
            await asyncio.sleep(random.uniform(0, 0.1))

class ConfirmationService:
    def __init__(self):
        self.dag = DAG()
        self.redis = Redis()
        self.rabbit = RabbitMQ()
        self.influx = InfluxDB()
        self.processor = ConfirmationProcessor(self.redis, self.dag, self.influx)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Confirmation process {os.getpid()} started")
            await self.processor.periodic_check_pool(stop_event)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.dag.connect()
        await self.influx.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.dag.close()
        await self.influx.close()
        logger.info("All connections closed")