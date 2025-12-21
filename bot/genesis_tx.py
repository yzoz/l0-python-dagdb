import asyncio
import httpx
import uuid
import json
import logging
from keys import pk
import sys
import os
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils.data import ordered_sum
from src.utils.crypto import sign_message

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

async def send(client, debit):
    c = "Hm4GGGtBGiLzHoVSTvXqz5JoV9JXqJWiYrZvXp6GCy4L"
    priv = "2kh7tB5iN6B8tvgz92jAsbonSqRXiaZFdaJ99uLyKnCL"
    d = debit
    a = 1_000_000_000_000  # сумма
    u = str(uuid.uuid4())  # уникальный идентификатор
    m = "Here's a tip for you"
    try:
        pool = await client.get('http://localhost:3000/pool')
        p = pool.json().get("nodes", [])
        # Формируем транзакцию
        tx = dict(
            parents=p,
            debit=d,
            credit=c,
            amount=a,
            uniq=u,
            msg=m
        )
        m = ordered_sum(tx)  # строка для подписи
        # Подпись транзакции
        s = sign_message(priv, m)
        tx_data = {
            "content": tx,
            "sign": s
        }
        logger.info(f"Sending transaction: {json.dumps(tx_data, indent=2)}")
        r = await client.post('http://localhost:8000/send', data=json.dumps(tx_data))
        logger.info(f"Response: {r.text}")
    except Exception as e:
        logger.error(f"Error in send: {e}")

async def main():
    async with httpx.AsyncClient() as client:
        for debit in pk:
            await send(client, debit)
            await asyncio.sleep(0.1)

if __name__ == "__main__":
    asyncio.run(main())