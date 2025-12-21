import asyncio
import httpx
import uuid
import logging
from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from src.utils.data import ordered_sum
from src.utils.crypto import sign_message
from src.models.tx import check_valid_key
import json

# Настройка логирования
logging.basicConfig(
    level=logging.WARNING,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
#logging.getLogger('telegram').setLevel(logging.WARNING)
#logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

# Конфигурация
HOST = "https://yat.io"

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /start"""
    await update.message.reply_text(
        "Привет! Я бот для отправки транзакций. Используй /faucet <address> для получения 1 монеты на указанный адрес."
    )

async def send_transaction(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    """Обработчик команды /faucet для отправки транзакции"""
    if not context.args:
        await update.message.reply_text("Ошибка: укажи адрес! Пример: /faucet <address>")
        return

    debit_address = context.args[0]
    
    # Валидация адреса получателя
    try:
        check_valid_key(debit_address)
    except ValueError as e:
        await update.message.reply_text(f"Ошибка: неверный формат адреса! {str(e)}")
        return

    async with httpx.AsyncClient() as client:
        try:
            credit = "Hm4GGGtBGiLzHoVSTvXqz5JoV9JXqJWiYrZvXp6GCy4L"
            priv = "2kh7tB5iN6B8tvgz92jAsbonSqRXiaZFdaJ99uLyKnCL"

            # Проверка, что адрес не совпадает с отправителем
            if debit_address == credit:
                await update.message.reply_text("Ошибка: нельзя отправить транзакцию самому себе!")
                return

            # Формирование транзакции
            amount = 1_000_000
            uniq = str(uuid.uuid4())
            msg = "Удачного тестирования!"

            # Получение пула родителей
            p = await client.get(f'{HOST}/pool')
            parents = p.json().get("nodes", [])

            tx = {
                "parents": parents,
                "debit": debit_address,
                "credit": credit,
                "amount": amount,
                "uniq": uniq,
                "msg": msg
            }
            message = ordered_sum(tx)
            signature = sign_message(priv, message)

            tx_data = {
                "content": tx,
                "sign": signature
            }

            logger.info(f"Sending transaction: {json.dumps(tx_data, indent=2)}")
            response = await client.post(f'{HOST}/send', data=json.dumps(tx_data))
            logger.info(f"Response: {response.text}")

            if response.status_code == 200:
                await update.message.reply_text(f"Транзакция отправлена! Uniq: {uniq}")
                print(f"SEND to {debit_address}")
            else:
                await update.message.reply_text(f"Ошибка при отправке: {response.text}")

        except Exception as e:
            logger.error(f"Error in send_transaction: {e}")
            await update.message.reply_text(f"Ошибка: {str(e)}")

async def main():
    """Основная функция для запуска бота"""
    bot_token = "TOKEN"
    application = Application.builder().token(bot_token).build()

    # Регистрация обработчиков команд
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("faucet", send_transaction))

    # Запуск бота
    logger.info("Starting Telegram bot...")
    try:
        await application.initialize()
        await application.start()
        await application.updater.start_polling(allowed_updates=Update.ALL_TYPES)
        # Держим цикл событий открытым
        await asyncio.Event().wait()
    finally:
        await application.updater.stop()
        await application.stop()
        await application.shutdown()

def run():
    """Запуск бота в текущем цикле событий или создание нового"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(loop.shutdown_asyncgens())
        loop.close()

if __name__ == "__main__":
    run()