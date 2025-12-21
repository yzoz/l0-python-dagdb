import logging
from dataclasses import dataclass
from src.database.mongodb import MongoDB
from src.database.redis import Redis
from src.database.influxdb import InfluxDB
from src.models.tx import TxWrite
from src.utils.profile import get_balance
import asyncio
import json
import os
import random
from pymongo.errors import DuplicateKeyError

logger = logging.getLogger(__name__)

@dataclass(frozen=True)
class Config:
    DEV_ADDR: set = (
        "9ui23Wuta5Sg9XSSmXmeQeRstaBm3fVK2JPz75zxZyPy",
        "RvCcJWKYpoEXGm6N72kDj3dA8zTxGTMSBpCLSKcJvdR",
        "2RAHCZg7SfyLY6x5uLmtDh1tNbZr4w5NTPMWXpHrqdyc"
    )
    MAX_KEYS_PER_RUN: int = 100  # Максимум ключей для обработки за раз

class WriterProcessor:
    def __init__(self, db: MongoDB, redis: Redis, influx: InfluxDB):
        self.db = db
        self.redis = redis
        self.influx = influx
        self.config = Config()

    async def process_confirmed(self):
        """Обрабатывает подтверждённые транзакции из Redis"""
        confirmed_keys = await self.redis.client.keys("confirmed:*")
        #confirmed_keys = []
        #async for key in self.redis.client.scan_iter("confirmed:*"):
        #    confirmed_keys.append(key)
        # Перемешиваем ключи и берём только часть
        random.shuffle(confirmed_keys)
        keys_to_process = confirmed_keys[:min(self.config.MAX_KEYS_PER_RUN, len(confirmed_keys))]
        for key in keys_to_process:
            tx_hash = key.split(":", 1)[1]
            try:
                #async with self.redis.client.lock(f"writer:{tx_hash}"):
                # Проверка, не обработана ли транзакция
                if await self.redis.client.exists(f"writed:{tx_hash}"):
                    await self.redis.client.delete(key)
                    logger.info(f"Transaction {tx_hash} already processed")
                    await self.influx.write_metric(
                        measurement="transaction_already_processed",
                        tags={"service": "writer"},
                        fields={"count": 1}
                    )
                    continue

                # Получение данных транзакции из Redis
                tx_data = await self.redis.client.get(key)
                if not tx_data:
                    logger.warning(f"Transaction data for {tx_hash} not found in Redis")
                    await self.redis.client.delete(key)
                    await self.influx.write_metric(
                        measurement="redis_not_found",
                        tags={"service": "writer"},
                        fields={"count": 1}
                    )
                    continue
                tx_data = json.loads(tx_data)

                # Создание объекта транзакции
                tx = self.create_tx(tx_hash, tx_data)

                # Проверка баланса
                balance = await get_balance(tx.credit, self.db, self.redis)
                if balance < tx.amount and "genesis" not in tx.msg:
                    logger.info(f"Insufficient funds for {tx.credit}: balance={balance}, amount={tx.amount}")
                    await self.redis.client.delete(key)
                    await self.influx.write_metric(
                        measurement="insufficient_funds",
                        tags={"service": "writer"},
                        fields={"count": 1}
                    )
                    continue

                # Запись в MongoDB
                success = await self.db.insert_tx(tx)
                if not success:
                    logger.error(f"Failed to save transaction {tx_hash} to MongoDB")
                    await self.influx.write_metric(
                        measurement="failed_to_save",
                        tags={"service": "writer"},
                        fields={"count": 1}
                    )
                    continue

                logger.info(f"Confirmed TX saved: {tx_hash}")

                # Обновление балансов в Redis
                balance_debit_key = f"balance:{tx.debit}"
                balance_credit_key = f"balance:{tx.credit}"
                async with self.redis.client.pipeline() as pipe:
                    await pipe.incrby(balance_debit_key, tx.amount)
                    await pipe.decrby(balance_credit_key, tx.amount)
                    await pipe.set(f"writed:{tx_hash}", 1, ex=3600)
                    try:
                        await pipe.execute()
                    except Exception as e:
                        logger.error(f"Pipeline failed for {tx_hash}: {e}")
                        await self.influx.write_metric(
                            measurement="pipeline_failed",
                            tags={"service": "writer"},
                            fields={"count": 1}
                        )
                        continue

                # Удаление ключа confirmed:
                await self.redis.client.delete(key)

                # Логирование в InfluxDB
                await self.influx.write_metric(
                    measurement="transaction_confirmed_count",
                    tags={"service": "writer"},
                    fields={"count": 1}
                )

            except DuplicateKeyError:
                logger.warning(f"Duplicate key error for hash: {tx_hash}")
                await self.redis.client.delete(key)
                await self.redis.client.set(f"writed:{tx_hash}", 1, ex=3600)
                await self.influx.write_metric(
                    measurement="duplicate_key_error",
                    tags={"service": "writer"},
                    fields={"count": 1}
                )

            except Exception as e:
                await self.influx.write_metric(
                    measurement="error_processing",
                    tags={"service": "writer"},
                    fields={"count": 1}
                )
                logger.error(f"Error processing transaction {tx_hash}: {e}")

    def create_tx(self, hash: str, data: dict) -> TxWrite:
        tx_dict = data
        tx_dict["hash"] = hash
        return TxWrite(**tx_dict)

    async def periodic_check_pool(self, stop_event=None):
        while not (stop_event and stop_event.is_set()):
            await self.process_confirmed()
            await asyncio.sleep(1)

class WriterService:
    def __init__(self):
        self.db = MongoDB()
        self.redis = Redis()
        self.influx = InfluxDB()
        self.processor = WriterProcessor(self.db, self.redis, self.influx)
        self.running = False

    async def run(self, stop_event=None) -> None:
        self.running = True
        try:
            await self.startup()
            logger.info(f"Writer process {os.getpid()} started")
            await self.processor.periodic_check_pool(stop_event)
        except Exception as e:
            logger.error(f"Service error: {e}", exc_info=True)
            raise
        finally:
            await self.shutdown()

    async def startup(self) -> None:
        await self.redis.connect()
        await self.db.connect()
        await self.influx.connect()

    async def shutdown(self) -> None:
        self.running = False
        await self.redis.close()
        await self.db.close()
        await self.influx.close()
        logger.info("All connections closed")