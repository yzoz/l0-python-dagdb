import logging
from aio_pika import connect_robust, Message, DeliveryMode

logger = logging.getLogger(__name__)

class RabbitMQ:
    def __init__(self):
        self.connection = None
        self.channel = None

    async def connect(self):
        """Инициализация подключения к RabbitMQ с dead-letter очередью."""
        self.connection = await connect_robust(f"amqp://admin:JQ2Y2dEUcGmmarKw9mDb2x9XXrVSKkst@localhost/")
        self.channel = await self.connection.channel()
        await self.channel.set_qos(prefetch_count=250) #TODO понять что делает
        # Создание dead-letter exchange
        dlx_exchange = await self.channel.declare_exchange(
            "dlx_exchange",
            type="direct",
            durable=True
        )
        
        # Создание dead-letter очереди
        dl_queue = await self.channel.declare_queue(
            "dead-letter",
            durable=True
        )
        
        # Привязка dead-letter очереди к exchange
        await dl_queue.bind(dlx_exchange, routing_key="tx")

    async def publish_message(self, queue_name, message: str):
        """Публикация сообщения в очередь."""
        await self.channel.default_exchange.publish(
            Message(message.encode("utf-8"), delivery_mode=DeliveryMode.PERSISTENT),
            routing_key=queue_name
        )
    
    async def consume_messages(self, queue_name: str, callback: callable, stop_event=None):
        """Потребление сообщений из очереди."""
        # Объявляем очередь с ограничением на 1000 сообщений и dead-letter конфигурацией
        queue = await self.channel.declare_queue(
            queue_name,
            durable=True,
            arguments={
                "x-max-length": 100,  # Ограничение на максимальное количество сообщений
                "x-dead-letter-exchange": "dlx_exchange",  # Exchange для отклоненных сообщений
                "x-dead-letter-routing-key": "tx"  # Routing key для dead-letter очереди
            }
        )
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                if stop_event and stop_event.is_set():
                    logger.info("Stop event detected, stopping message consumption")
                    break
                await callback(message)

    async def retry_dead_letter(self):
        """Попытка повторной отправки сообщений из dead-letter очереди в tx."""
        queue = await self.channel.declare_queue("dead-letter", durable=True)
        async with queue.iterator() as queue_iter:
            async for message in queue_iter:
                try:
                    await self.publish_message("tx", message.body.decode())
                    await message.ack()
                    logger.info("Successfully retried dead-letter message")
                except Exception as e:
                    logger.error(f"Failed to retry dead-letter message: {str(e)}")
                    await message.nack(requeue=True)  # Возвращаем в dead-letter, если не удалось

    async def close(self):
        """Закрытие подключения."""
        if self.connection and not self.connection.is_closed:
            await self.connection.close()