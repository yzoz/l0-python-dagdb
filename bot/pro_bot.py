import asyncio
import aiohttp
import uuid
import json
import logging
from multiprocessing import Process
import sys
import os
import time
import random
from typing import Optional
from keys import pk, sk

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils.data import hash_hex, ordered_sum
from src.utils.crypto import sign_message

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Отключаем подробные HTTP логи от aiohttp
logging.getLogger("aiohttp.access").setLevel(logging.WARNING)
logging.getLogger("aiohttp.client").setLevel(logging.WARNING)
logging.getLogger("aiohttp.server").setLevel(logging.WARNING)

start = time.time()

# Конфигурация для обработки ошибок
MAX_RETRIES = 5
BASE_DELAY = 1  # базовая задержка в секундах
MAX_DELAY = 10  # максимальная задержка

# Настройки таймаутов для aiohttp
TIMEOUT_CONFIG = aiohttp.ClientTimeout(
    total=5.0,      # общий таймаут
    connect=1.0,    # таймаут подключения
    sock_read=2.0,  # таймаут чтения из сокета
    sock_connect=1.0 # таймаут подключения к сокету
)

async def arange(count):
    for i in range(count):
        yield i
        await asyncio.sleep(0.1)
        if i % 2 == 0: await asyncio.sleep(round(random.uniform(0, 0.25), 3))
        if i % 3 == 0: await asyncio.sleep(round(random.uniform(0, 0.5), 3))
        if i % 5 == 0: await asyncio.sleep(round(random.uniform(0, 1.0), 3))
        if i % 10 == 0: await asyncio.sleep(round(random.uniform(0, 2.5), 3))
        if i % 25 == 0: await asyncio.sleep(round(random.uniform(0, 5.0), 3))

async def exponential_backoff(attempt: int) -> float:
    """Экспоненциальная задержка с джиттером."""
    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
    # Добавляем джиттер (случайность) для избежания thundering herd
    jitter = delay * 0.1 * (0.5 - asyncio.get_event_loop().time() % 1)
    return delay + jitter

async def safe_request(session: aiohttp.ClientSession, method: str, url: str, **kwargs) -> Optional[aiohttp.ClientResponse]:
    """Безопасный HTTP запрос с повторными попытками и обработкой ошибок."""
    for attempt in range(MAX_RETRIES):
        try:
            if method.upper() == 'GET':
                response = await session.get(url, **kwargs)
            elif method.upper() == 'POST':
                response = await session.post(url, **kwargs)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            # Проверяем статус код
            if response.status >= 500:
                logger.warning(f"Server error {response.status}, attempt {attempt + 1}/{MAX_RETRIES}")
                response.close()
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(await exponential_backoff(attempt))
                    continue
            
            return response
            
        except aiohttp.ClientConnectorError as e:
            logger.warning(f"Connection error: {e}, attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                logger.error(f"Failed to connect after {MAX_RETRIES} attempts")
                return None
                
        except asyncio.TimeoutError as e:
            logger.warning(f"Timeout error: {e}, attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                logger.error(f"Request timed out after {MAX_RETRIES} attempts")
                return None
                
        except aiohttp.ClientResponseError as e:
            logger.warning(f"HTTP status error: {e.status}, attempt {attempt + 1}/{MAX_RETRIES}")
            if e.status >= 500 and attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                logger.error(f"HTTP error: {e}")
                return None
                
        except aiohttp.ClientError as e:
            logger.warning(f"Client error: {e}, attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                logger.error(f"Client error after {MAX_RETRIES} attempts: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                return None
    
    return None

async def send(t, session, host):
    """Отправка транзакций с обработкой ошибок."""
    success_count = 0
    error_count = 0
    
    async for i in arange(int(t)):
        try:
            # Получаем пул узлов
            pool_response = await safe_request(session, 'GET', f'{host}/pool')
            if pool_response is None:
                error_count += 1
                logger.debug(f"Failed to get pool, skipping transaction {i}")
                continue
            
            try:
                pool_data = await pool_response.json()
                pool_response.close()
            except (json.JSONDecodeError, ValueError, aiohttp.ContentTypeError) as e:
                error_count += 1
                logger.warning(f"Failed to parse pool response: {e}")
                pool_response.close()
                continue
            n = len(sk) - 1
            id = random.randint(0, n)
            c = pk[id]  # credit (отправитель)
            priv = sk[id]  # приватный ключ
            d = pk[random.randint(0, n)]  # debit (получатель)
            if c == d: continue
            a = random.randint(1, 1000000000)  # сумма
            # TODO сделать уже нормально UNIQ
            u = str(uuid.uuid4())  # уникальный идентификатор
            # TODO генерить уникальные текстовые сообщения
            m = str(random.randint(0, 100000))  # сообщение
            p = pool_data.get("nodes", [])
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
            signed = dict(tx, sign=s)
            # Формируем транзакцию
            tx_hash = hash_hex(ordered_sum(signed))
            tx_with_time = dict(signed, time=int(time.time_ns() / 1000))
            tx_data = {
                "hash": tx_hash,
                "parents": p,
                "data": tx_with_time
            }
            # Отправляем транзакцию
            add_response = await safe_request(session, 'POST', f'{host}/add', json=tx_data)
            if add_response is None:
                error_count += 1
                logger.debug(f"Failed to add transaction {i}")
                continue
            
            add_response.close()
            success_count += 1
            if success_count % 100 == 0:  # Логируем каждые 100 успешных транзакций
                logger.info(f"Successfully sent {success_count} transactions")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Unexpected error in transaction {i}: {e}", exc_info=True)
    
    logger.info(f"Completed: {success_count} successful, {error_count} failed transactions")

async def run_sender(t, c, h):
    """Запуск C параллельных корутин в одном процессе."""
    # Настройки для aiohttp connector
    connector = aiohttp.TCPConnector(
        limit=20,           # максимум общих соединений
        limit_per_host=10,  # максимум соединений на хост
        ttl_dns_cache=300,  # время жизни DNS кеша
        use_dns_cache=True,
        keepalive_timeout=5.0,  # время жизни keep-alive соединений
        enable_cleanup_closed=True
    )
    
    async with aiohttp.ClientSession(
        timeout=TIMEOUT_CONFIG,
        connector=connector,
        headers={'Content-Type': 'application/json'}
    ) as session:
        await asyncio.gather(*[send(t, session, h) for _ in range(c)])

def process_target(t, c, h):
    """Функция-цель для каждого процесса."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_sender(t, c, h))
    except KeyboardInterrupt:
        logger.info(f"Process interrupted")
    except Exception as e:
        logger.error(f"Process error: {e}", exc_info=True)
    finally:
        loop.close()

def main():
    args = sys.argv[1:]

    try:
        t = int(args[0])
    except (IndexError, ValueError):
        t = 10_000_000
    try:
        c = int(args[1])
    except (IndexError, ValueError):
        c = 1
    try:
        n = int(args[2])
    except (IndexError, ValueError):
        n = 1
    try:
        h = str(args[3])
    except (IndexError, ValueError):
        h = "http://localhost:3000"

    logger.info(f"Starting with parameters: transactions={t}, coroutines={c}, processes={n}, host={h}")

    """Основная функция для запуска многопроцессного генератора транзакций."""
    num_processes = n

    processes = []
    for i in range(num_processes):
        p = Process(target=process_target, args=(t, c, h))
        processes.append(p)
        p.start()
        logger.info(f"Started process {i+1}/{num_processes}")

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Main process interrupted, terminating all processes...")
        for p in processes:
            if p.is_alive():
                p.terminate()
                p.join()
        sys.exit(0)

if __name__ == "__main__":
    main()
    total_time = int(time.time() - start)
    print(f"Total execution time: {total_time} seconds")