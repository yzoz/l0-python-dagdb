import asyncio
import json
from src.models.tx import TxStore
from src.utils.data import hash_hex, ordered_sum
from pymongo import MongoClient, IndexModel, ASCENDING
import redis.asyncio as redis
from aio_pika import connect_robust
import time
import logging

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def main():
    # Инициализация подключений
    redis_client = redis.Redis(host="localhost", port=6379, password="JQ2Y2dEUcGmmarKw9mDb2x9XXrVSKkst", decode_responses=True)
    mongo_client = MongoClient('mongodb://localhost:27017', username="admin", password="JQ2Y2dEUcGmmarKw9mDb2x9XXrVSKkst")
    rabit_client = await connect_robust(f"amqp://admin:JQ2Y2dEUcGmmarKw9mDb2x9XXrVSKkst@localhost/")
    db = mongo_client.feo
    txs = db.tx

    async with rabit_client:
        try:
            channel = await rabit_client.channel()
            await channel.queue_delete("tx")
            logger.info("Deleted RabbitMQ queue 'tx'")
        except Exception as e:
            logger.error(f"Failed to delete RabbitMQ queue 'tx': {str(e)}")

    # Очистка MongoDB
    txs.drop()
    logger.info("MongoDB collection 'tx' dropped")
    txs.create_indexes([IndexModel([("hash", ASCENDING)], unique=True)])
    txs.create_indexes([IndexModel([("credit", ASCENDING), ("uniq", 1)])])

    # Подключение к Redis
    await redis_client.flushall()
    logger.info("Redis flushed")

    # Определение генезис-транзакций
    genesis_txs = [
        dict(
            prnts=[],
            addr="",
            seq=0,
            var=dict(
                ca="0",
                to="Hm4GGGtBGiLzHoVSTvXqz5JoV9JXqJWiYrZvXp6GCy4L",
                val=500_000_000_000_000,
                msg="genesis",
            )
        ),
        dict(
            prnts=[],
            addr="",
            seq=0,
            var=dict(
                ca="0",
                to="RvCcJWKYpoEXGm6N72kDj3dA8zTxGTMSBpCLSKcJvdR",
                val=330000000000000,
                msg="genesis2",
            )
        ),
        dict(
            prnts=[],
            addr="",
            seq=0,
            var=dict(
                ca="0",
                to="2RAHCZg7SfyLY6x5uLmtDh1tNbZr4w5NTPMWXpHrqdyc",
                val=250000000000000,
                msg="genesis3",
            )
        ),
    ]

    try:

        # Добавление генезис-транзакций
        genesis_json = []
        for tx in genesis_txs:
            tx_dict = dict(tx)
            tx_hash = hash_hex(ordered_sum(tx_dict))
            tx_data = {
                "tx": tx_dict,
                "sign": "",
                "func": "transferToken",
            }
            
            genesis_json.append({"hash": tx_hash, "data": tx_data}) 

            # Установка баланса в Redis
            """await redis_client.set(f"balance:{tx.debit}", tx.amount)
            logger.info(f"Created transaction {tx_hash} with balance {tx.amount} for {tx.debit}")"""

        # Сохранение генезис-транзакций в genesis.json
        with open("db/genesis.json", "w") as f:
            json.dump(genesis_json, f, indent=2)

    except Exception as e:
        logger.error(f"Error in genesis setup: {str(e)}")
        raise

    finally:
        await redis_client.aclose()
        mongo_client.close()
        logger.info("All connections closed")

if __name__ == "__main__":
    asyncio.run(main())