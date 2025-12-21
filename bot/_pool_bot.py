import asyncio
import httpx
import uuid
import json
import logging
from multiprocessing import Process
import sys
import time
from typing import Optional

# Настройка логирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Отключаем подробные HTTP логи от httpx
logging.getLogger("httpx").setLevel(logging.WARNING)
# Также отключаем логи от httpcore (внутренняя библиотека httpx)
logging.getLogger("httpcore").setLevel(logging.WARNING)

start = time.time()

HOST = "http://192.168.1.85:3000"

# Конфигурация для обработки ошибок
MAX_RETRIES = 5
BASE_DELAY = 1  # базовая задержка в секундах
MAX_DELAY = 10  # максимальная задержка
TIMEOUT_CONFIG = httpx.Timeout(
    timeout=5.0,  # общий таймаут
    connect=1.0,  # таймаут подключения
    read=2.0,     # таймаут чтения
    write=1.0     # таймаут записи
)

async def arange(count):
    """Генератор для асинхронного перебора чисел.""" 
    for i in range(count):
        yield i
        await asyncio.sleep(0.1)

async def exponential_backoff(attempt: int) -> float:
    """Экспоненциальная задержка с джиттером."""
    delay = min(BASE_DELAY * (2 ** attempt), MAX_DELAY)
    # Добавляем джиттер (случайность) для избежания thundering herd
    jitter = delay * 0.1 * (0.5 - asyncio.get_event_loop().time() % 1)
    return delay + jitter

async def safe_request(client: httpx.AsyncClient, method: str, url: str, **kwargs) -> Optional[httpx.Response]:
    """Безопасный HTTP запрос с повторными попытками и обработкой ошибок."""
    for attempt in range(MAX_RETRIES):
        try:
            if method.upper() == 'GET':
                response = await client.get(url, **kwargs)
            elif method.upper() == 'POST':
                response = await client.post(url, **kwargs)
            else:
                raise ValueError(f"Unsupported method: {method}")
            
            # Проверяем статус код
            if response.status_code >= 500:
                logger.warning(f"Server error {response.status_code}, attempt {attempt + 1}/{MAX_RETRIES}")
                if attempt < MAX_RETRIES - 1:
                    await asyncio.sleep(await exponential_backoff(attempt))
                    continue
            
            return response
            
        except httpx.ConnectError as e:
            logger.warning(f"Connection error: {e}, attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                logger.error(f"Failed to connect after {MAX_RETRIES} attempts")
                return None
                
        except httpx.TimeoutException as e:
            logger.warning(f"Timeout error: {e}, attempt {attempt + 1}/{MAX_RETRIES}")
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                logger.error(f"Request timed out after {MAX_RETRIES} attempts")
                return None
                
        except httpx.HTTPStatusError as e:
            logger.warning(f"HTTP status error: {e.response.status_code}, attempt {attempt + 1}/{MAX_RETRIES}")
            if e.response.status_code >= 500 and attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                logger.error(f"HTTP error: {e}")
                return None
                
        except Exception as e:
            logger.error(f"Unexpected error: {e}", exc_info=True)
            if attempt < MAX_RETRIES - 1:
                await asyncio.sleep(await exponential_backoff(attempt))
                continue
            else:
                return None
    
    return None

async def send(t, client):
    """Отправка транзакций с обработкой ошибок."""
    success_count = 0
    error_count = 0
    
    async for i in arange(int(t)):
        try:
            # Получаем пул узлов
            pool_response = await safe_request(client, 'GET', f'{HOST}/pool')
            if pool_response is None:
                error_count += 1
                logger.debug(f"Failed to get pool, skipping transaction {i}")
                continue
            
            try:
                pool_data = pool_response.json()
            except (json.JSONDecodeError, ValueError) as e:
                error_count += 1
                logger.warning(f"Failed to parse pool response: {e}")
                continue
            
            # Формируем транзакцию
            tx = dict(
                parents=pool_data.get("nodes", []),
                hash=str(uuid.uuid4()),
            )
            
            # Отправляем транзакцию
            add_response = await safe_request(client, 'POST', f'{HOST}/add', json=tx)
            if add_response is None:
                error_count += 1
                logger.debug(f"Failed to add transaction {i}")
                continue
            
            success_count += 1
            if success_count % 100 == 0:  # Логируем каждые 100 успешных транзакций
                logger.info(f"Successfully sent {success_count} transactions")
                
        except Exception as e:
            error_count += 1
            logger.error(f"Unexpected error in transaction {i}: {e}", exc_info=True)
    
    logger.info(f"Completed: {success_count} successful, {error_count} failed transactions")

async def run_sender(t, c):
    """Запуск C параллельных корутин в одном процессе."""
    # Настройки для httpx клиента
    limits = httpx.Limits(
        max_keepalive_connections=10,  # максимум keep-alive соединений
        max_connections=20,            # максимум общих соединений
        keepalive_expiry=5.0         # время жизни keep-alive соединений
    )
    
    async with httpx.AsyncClient(
        timeout=TIMEOUT_CONFIG,
        limits=limits,
        follow_redirects=True
    ) as client:
        await asyncio.gather(*[send(t, client) for _ in range(c)])

def process_target(t, c):
    """Функция-цель для каждого процесса."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_sender(t, c))
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
        t = 1_000_000
    try:
        c = int(args[1])
    except (IndexError, ValueError):
        c = 1
    try:
        n = int(args[2])
    except (IndexError, ValueError):
        n = 1

    logger.info(f"Starting with parameters: transactions={t}, coroutines={c}, processes={n}")

    """Основная функция для запуска многопроцессного генератора транзакций."""
    num_processes = n

    processes = []
    for i in range(num_processes):
        p = Process(target=process_target, args=(t, c))
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