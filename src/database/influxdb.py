import logging
from influxdb_client.client.influxdb_client_async import InfluxDBClientAsync
from influxdb_client import WritePrecision

logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

class InfluxDB:
    def __init__(self):
        self.client = None
        self.bucket = "stat"
        self.org = "yat"

    async def connect(self):
        """Асинхронно подключается к InfluxDB"""
        try:
            self.client = InfluxDBClientAsync(
                url="http://localhost:8086",
                token="WdTB65aKLfT-ip4fdnNXmhpQeqj7Zp4xhgdav_YQg0vkVn82YB7_HP4P3zEGp2O28czwPKKPRzfAsop_2paj5w==",
                org=self.org,
                timeout=5000
            )
            # Проверка готовности
            await self.client.ping()
            logger.info("Connected to InfluxDB")
        except Exception as e:
            logger.error(f"Failed to connect to InfluxDB: {e}")

    async def write_metric(self, measurement: str, tags: dict, fields: dict):
        """Асинхронно записывает метрику в InfluxDB"""
        try:
            write_api = self.client.write_api()
            point = {
                "measurement": measurement,
                "tags": tags,
                "fields": fields,
                "time": None  # Автоматическая метка времени
            }
            await write_api.write(
                bucket=self.bucket,
                record=point,
                write_precision=WritePrecision.NS
            )
            logger.debug(f"Metric written: {measurement}, tags: {tags}, fields: {fields}")
        except Exception as e:
            logger.error(f"Failed to write metric: {e}")

    async def close(self):
        """Асинхронно закрывает соединение с InfluxDB"""
        try:
            if self.client:
                await self.client.__aexit__(None, None, None)
            logger.info("InfluxDB connection closed")
        except Exception as e:
            logger.error(f"Failed to close InfluxDB: {e}")
        finally:
            self.client = None