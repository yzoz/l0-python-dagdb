import aiohttp
import asyncio
import logging
from typing import Dict, Any, Optional, List
from aiohttp import ClientSession, ClientTimeout, ClientResponse, TCPConnector

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

class DAG:
    """Класс для взаимодействия с API DAG-сервера через aiohttp."""

    # Константы для ретраев и таймаутов
    BASE_DELAY = 1.0  # Базовая задержка в секундах
    MAX_DELAY = 10.0  # Максимальная задержка в секундах
    MAX_RETRIES = 3   # Максимальное количество попыток

    def __init__(self):
        """
        Инициализация клиента API.

        Args:
            base_url (str): Базовый URL сервера (например, 'http://localhost:3000').
            timeout (int): Общий таймаут запроса в секундах.
            max_retries (int): Максимальное количество повторных попыток.
        """
        self.base_url = "http://localhost:3000"
        self.timeout = ClientTimeout(
            total=5.0,      # общий таймаут
            connect=1.0,    # таймаут подключения
            sock_read=2.0,  # таймаут чтения из сокета
            sock_connect=1.0 # таймаут подключения к сокету
        )
        self.max_retries = 5
        # Настройки для aiohttp connector
        self.connector = TCPConnector(
            limit=200,           # максимум общих соединений
            limit_per_host=100,  # максимум соединений на хост
            ttl_dns_cache=300,  # время жизни DNS кеша
            use_dns_cache=True,
            keepalive_timeout=5.0,  # время жизни keep-alive соединений
            enable_cleanup_closed=True
        )
        self.session: Optional[ClientSession] = None

    async def exponential_backoff(self, attempt: int) -> float:
        """Экспоненциальная задержка с джиттером."""
        delay = min(self.BASE_DELAY * (2 ** attempt), self.MAX_DELAY)
        # Добавляем джиттер для избежания thundering herd
        jitter = delay * 0.1 * (0.5 - asyncio.get_event_loop().time() % 1)
        return delay + jitter

    async def safe_request(self, method: str, endpoint: str, **kwargs) -> Optional[Dict[str, Any]]:
        """
        Безопасный HTTP-запрос с повторными попытками и обработкой ошибок.

        Args:
            method (str): HTTP-метод ('GET', 'POST').
            endpoint (str): Конечная точка API (например, '/add').
            **kwargs: Дополнительные параметры запроса (json, params и т.д.).

        Returns:
            Optional[Dict[str, Any]]: Ответ сервера в формате JSON или None при ошибке.
        """
        #await self.start_session()
        url = f"{self.base_url}{endpoint}"

        for attempt in range(self.max_retries):
            try:
                if method.upper() == 'GET':
                    response = await self.session.get(url, **kwargs)
                elif method.upper() == 'POST':
                    response = await self.session.post(url, **kwargs)
                else:
                    raise ValueError(f"Unsupported method: {method}")

                # Проверяем статус код
                if response.status >= 500:
                    logger.warning(f"Server error {response.status} at {url}, attempt {attempt + 1}/{self.max_retries}")
                    await response.release()
                    if attempt < self.max_retries - 1:
                        await asyncio.sleep(await self.exponential_backoff(attempt))
                        continue
                    logger.error(f"Server error {response.status} after {self.max_retries} attempts at {url}")
                    return None

                if response.status >= 400:
                    error_body = await response.text()
                    logger.warning(f"Client error {response.status} at {url}: {error_body}")
                    return None

                return await response.json()

            except aiohttp.ClientConnectorError as e:
                logger.warning(f"Connection error at {url}: {e}, attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(await self.exponential_backoff(attempt))
                    continue
                logger.error(f"Failed to connect after {self.max_retries} attempts at {url}: {e}")
                return None

            except asyncio.TimeoutError as e:
                logger.warning(f"Timeout error at {url}: {e}, attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(await self.exponential_backoff(attempt))
                    continue
                logger.error(f"Request timed out after {self.max_retries} attempts at {url}")
                return None

            except aiohttp.ClientResponseError as e:
                logger.warning(f"HTTP status error at {url}: {e.status}, attempt {attempt + 1}/{self.max_retries}")
                if e.status >= 500 and attempt < self.max_retries - 1:
                    await asyncio.sleep(await self.exponential_backoff(attempt))
                    continue
                logger.error(f"HTTP error at {url}: {e}")
                return None

            except aiohttp.ClientError as e:
                logger.warning(f"Client error at {url}: {e}, attempt {attempt + 1}/{self.max_retries}")
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(await self.exponential_backoff(attempt))
                    continue
                logger.error(f"Client error after {self.max_retries} attempts at {url}: {e}")
                return None

            except Exception as e:
                logger.error(f"Unexpected error at {url}: {e}", exc_info=True)
                if attempt < self.max_retries - 1:
                    await asyncio.sleep(await self.exponential_backoff(attempt))
                    continue
                return None

        return None

    async def connect(self) -> None:
        """Запускает новую сессию aiohttp, если она еще не активна."""
        if self.session is None or self.session.closed:
            self.session = ClientSession(
                timeout=self.timeout,
                connector=self.connector,
                headers={'Content-Type': 'application/json'}
            )
            logger.debug(f"Started new aiohttp session for {self.base_url}")

    async def close(self) -> None:
        """Закрывает активную сессию aiohttp."""
        if self.session and not self.session.closed:
            await self.session.close()
            logger.debug(f"Closed aiohttp session for {self.base_url}")

    async def add_node(self, hash: str, parents: List[str], data: Any) -> Optional[Dict[str, Any]]:
        """
        Добавляет новый узел в DAG через эндпоинт /add.

        Args:
            hash (str): Хэш узла.
            parents (List[str]): Список хэшей родительских узлов.
            data (Any): Данные узла (сериализуемые в JSON).

        Returns:
            Optional[Dict[str, Any]]: Ответ сервера, например: {"status": "success"} или None при ошибке.
        """
        payload = {
            "hash": hash,
            "parents": parents,
            "data": data
        }
        logger.info(f"Adding node {hash} with parents {parents}")
        response = await self.safe_request("POST", "/add", json=payload)
        if response and response.get("status") == "success":
            logger.info(f"Node {hash} added successfully")
        elif response:
            logger.error(f"Failed to add node {hash}: {response.get('message')}")
        return response

    async def get_weights(self) -> Optional[Dict[str, Any]]:
        """
        Получает веса узлов через эндпоинт /weights.

        Returns:
            Optional[Dict[str, Any]]: Ответ сервера, например: {"status": "success", "nodes": [...]} или None.
        """
        response = await self.safe_request("GET", "/weights")
        if response and response.get("status") == "success":
            logger.debug(f"Received {len(response.get('nodes', []))} node weights")
        elif response:
            logger.error(f"Failed to fetch weights: {response.get('message')}")
        return response

    async def remove_nodes(self, hashes: List[str]) -> Optional[Dict[str, Any]]:
        """
        Удаляет узлы из DAG через эндпоинт /remove.

        Args:
            hashes (List[str]): Список хэшей узлов для удаления.

        Returns:
            Optional[Dict[str, Any]]: Ответсервера, например: {"status": "success"} или None.
        """
        payload = {"hashes": hashes}
        logger.info(f"Removing nodes {hashes}")
        response = await self.safe_request("POST", "/remove", json=payload)
        if response and response.get("status") == "success":
            logger.info(f"Nodes {hashes} removed successfully")
        elif response:
            logger.error(f"Failed to remove nodes {hashes}: {response.get('message')}")
        return response

    async def __aenter__(self) -> "DAG":
        """Поддержка асинхронного контекстного менеджера (async with)."""
        await self.connect()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        """Закрытие сессии при выходе из контекстного менеджера."""
        await self.close()

# Пример использования
async def main():
    async with DAG() as client:
        # Добавление узла
        add_response = await client.add_node("hash1", ["parent1", "parent2"], {"key": "value"})
        print("Add node response:", add_response)

        # Получение весов
        weights_response = await client.get_weights()
        print("Weights:", weights_response)

        # Удаление узлов
        remove_response = await client.remove_nodes(["hash1"])
        print("Remove nodes response:", remove_response)

if __name__ == "__main__":
    asyncio.run(main())