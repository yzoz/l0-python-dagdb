import asyncio
import json
import time
import logging
from aio_pika import IncomingMessage
from src.database.queues import RabbitMQ
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.database.graph import DAG
from src.database.influxdb import InfluxDB
from src.models.tx import TxStore
from src.utils.data import hash_hex, ordered_sum
from src.utils.profile import get_balance
import os

logger = logging.getLogger(__name__)
class ConsumerProcessor:
    """Класс, отвечающий за обработку транзакций и управление графом   """ 
    def __init__(self, rabbit: RabbitMQ, db: MongoDB, redis: Redis, dag: DAG, influx: InfluxDB):
        self.db = db
        self.rabbit = rabbit
        self.redis = redis
        self.dag = dag
        self.influx = influx

    async def process_message(self, message: IncomingMessage) -> None:
        """Обрабатывает входящее сообщение из RabbitMQ"""
        try:
            tx = self.parse_message(message)
            await self.process_transaction(tx, message)
        except json.JSONDecodeError:
            logger.error("Failed to parse message: Invalid JSON format")
            await message.nack(requeue=False)
        except asyncio.TimeoutError:
            logger.error("Timeout during processing")
            await message.nack(requeue=True)
        except Exception as e:
            logger.error(f"Unexpected processing error: {e}", exc_info=True)
            await message.nack(requeue=False)

    def parse_message(self, message: IncomingMessage) -> TxStore:
        """Преобразует сырые данные сообщения в объект TxStore"""
        data = json.loads(message.body.decode("utf-8"))
        content = data["content"]
        content["sign"] = data["sign"]
        return TxStore(**content)

    async def process_transaction(self, tx: TxStore, message: IncomingMessage) -> None:
        """Обрабатывает одну транзакцию"""
        tx_hash = hash_hex(ordered_sum(dict(tx)))
        async with self.redis.client.lock(f"consumer:{tx_hash}"):
            # Проверка дубликатов
            if await self.check_duplicate(tx):
                logger.info(f"Duplicate transaction: {tx_hash}")
                await message.nack(requeue=False)
                return

            # Проверка баланса
            if not await self.verify_balance(tx):
                logger.info(f"Insufficient funds for {tx.credit}")
                await message.nack(requeue=False)
                return

            # Проверка наличия родителей
            if not await self.verify_parents(tx_hash, tx):
                await message.nack(requeue=False)
                return

            # Проверка и добавление в граф
            if not await self.add_to_graph(tx_hash, tx):
                await message.nack(requeue=False)
                return

            await message.ack()
            #logger.info(f"Transaction {tx_hash} processed and added to graph")

    async def check_duplicate(self, tx: TxStore) -> bool:
        """Проверяет, существует ли уже такая транзакция в базе"""
        exists = await self.db.find_one({"credit": tx.credit, "uniq": tx.uniq})
        return bool(exists)

    async def verify_balance(self, tx: TxStore) -> bool:
        """Проверяет, достаточно ли средств для выполнения транзакции"""
        balance = await get_balance(tx.credit, self.db, self.redis)
        """if tx.credit == "8Aq9TxgVtGCLcpyTmJpqj7D6a9WriTVHAmjEvdDxaHfJ":
            print(tx.credit, balance, tx.amount, balance - tx.amount)"""
        return balance >= tx.amount
    
    async def verify_parents(self, tx_hash: str, tx: TxStore) -> bool:
        """Проверяет наличие всех родительских транзакций по порядку и их подтверждение в Redis"""
        '''for parent_hash in tx.parents:
            # Проверка существования родителя в Memgraph
            async with self.dag.driver.session() as session:
                result = await session.run(
                    """
                    MATCH (t:Transaction {hash: $parent_hash})
                    RETURN t
                    """,
                    parent_hash=parent_hash
                )
                record = await result.single()
                if not record:
                    logger.info(f"Parent transaction {parent_hash} not found for {tx_hash} in Graph")
                    # Проверка подтверждения родителя в Redis
                    confirmed_key = f"confirmed:{parent_hash}"
                    if not await self.redis.client.exists(confirmed_key) and not await self.redis.client.exists(f"writed:{parent_hash}"):
                        await self.influx.write_metric(
                            measurement="parent_transaction_not_found",
                            tags={
                                "service": "consumer"
                            },
                            fields={
                                "count": 1
                            }
                        )
                        logger.warning(f"Parent transaction {parent_hash} not found for {tx_hash}")
                        return False'''
        return True
    
    async def add_to_graph(self, tx_hash: str, tx: TxStore) -> bool:
        """Добавляет транзакцию в граф через DAG API"""
        async with self.redis.client.lock(f"dag:{tx_hash}"):
            # Подготовка данных транзакции
            # TODO подумать, может подставлять время автоматом в pydantic
            now = int(time.time_ns() / 1000)
            tx_data = dict(tx, time=now)
            
            # Вызов API для добавления узла
            response = await self.dag.add_node(
                hash=tx_hash,
                parents=tx.parents,
                data=tx_data
            )
            
            if response and response.get("status") == "success":
                # Логирование успешного добавления
                await self.influx.write_metric(
                    measurement="transaction_added_count",
                    tags={"service": "consumer"},
                    fields={"count": 1}
                )
                return True
            else:
                # Логирование ошибки
                error_msg = response.get("message") if response else "Unknown error"
                logger.warning(f"Failed to add transaction {tx_hash} to graph: {error_msg}")
                if "Cycle detected" in error_msg:
                    await self.influx.write_metric(
                        measurement="cycle_detected",
                        tags={"service": "consumer"},
                        fields={"count": 1}
                    )
                elif "No existing parents" in error_msg:
                    await self.influx.write_metric(
                        measurement="parent_transaction_not_found",
                        tags={"service": "consumer"},
                        fields={"count": 1}
                    )
                return False

class ConsumerService:
    def __init__(self):
        self.db = MongoDB()
        self.rabbit = RabbitMQ()
        self.redis = Redis()
        self.dag = DAG()
        self.influx = InfluxDB()
        self.processor = ConsumerProcessor(self.rabbit, self.db, self.redis, self.dag, self.influx)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Consumer process {os.getpid()} started")
            await self.rabbit.consume_messages("tx", self.processor.process_message, stop_event=stop_event)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.rabbit.connect()
        await self.db.connect()
        await self.dag.connect()
        await self.influx.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        if self.rabbit.connection and not self.rabbit.connection.is_closed:
            await self.rabbit.close()
            logger.info("RabbitMQ connection closed")
        await self.db.close()
        await self.dag.close()
        await self.influx.close()
        logger.info("All connections closed")