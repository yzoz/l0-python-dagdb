import asyncio
import httpx
import random
import json
import logging
from multiprocessing import Process
import sys
import os
import time
from keys import pk, sk

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
from src.utils.data import ordered_sum
from src.utils.crypto import sign_message

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(process)d - %(levelname)s - %(message)s'
)
# Отключаем подробные HTTP логи от httpx
logging.getLogger("httpx").setLevel(logging.WARNING)
# Также отключаем логи от httpcore (внутренняя библиотека httpx)
logging.getLogger("httpcore").setLevel(logging.WARNING)
logger = logging.getLogger(__name__)

start = time.time()

async def arange(count):
    for i in range(count):
        yield i
        #await asyncio.sleep(0.001)

async def realistic_transaction_generator(count, base_tps=250):
    """
    Генератор транзакций с реалистичным поведением:
    - Базовая интенсивность с флуктуациями
    - Периодические всплески активности
    - Случайные кластеры транзакций
    - Суточные и недельные паттерны (опционально)
    """
    
    # Параметры системы
    min_tps = 100        # Минимальная интенсивность
    max_tps = 1000       # Максимальная интенсивность
    current_tps = base_tps
    
    # Параметры для флуктуаций
    trend_change_prob = 0.005    # Вероятность смены тренда
    small_change_prob = 0.1      # Вероятность малого изменения
    cluster_prob = 0.02          # Вероятность кластера
    
    # Состояние тренда
    trend_direction = 0  # -1, 0, 1
    trend_strength = 0
    
    last_time = time.time()
    
    for i in range(count):
        current_time = time.time()
        
        # Обновляем интенсивность только если прошло достаточно времени
        if current_time - last_time > 0.1:  # Каждые 100мс
            # Большие изменения тренда (редко)
            if random.random() < trend_change_prob:
                trend_direction = random.choice([-1, 0, 1])
                trend_strength = random.uniform(0.5, 2.0)
                #print(f"Смена тренда: направление={trend_direction}, сила={trend_strength:.2f}")
            
            # Применяем тренд
            if trend_direction != 0:
                trend_change = trend_direction * trend_strength * random.uniform(0.8, 1.2)
                current_tps += trend_change
                trend_strength *= 0.995  # Постепенное затухание тренда
            
            # Малые случайные флуктуации
            if random.random() < small_change_prob:
                current_tps += random.uniform(-5, 5)
            
            # Ограничиваем диапазон
            current_tps = max(min_tps, min(current_tps, max_tps))
            last_time = current_time
        
        # Проверяем на кластер транзакций
        if random.random() < cluster_prob:
            cluster_size = random.randint(3, 15)
            cluster_intensity = current_tps * random.uniform(2.0, 5.0)  # Увеличиваем интенсивность
            
            #print(f"Кластер: размер={cluster_size}, интенсивность={cluster_intensity:.1f} tps")
            
            for j in range(cluster_size):
                # Интервал внутри кластера
                cluster_delay = random.expovariate(cluster_intensity)
                await asyncio.sleep(cluster_delay)
                yield i
                
                # Прерываем кластер если достигли лимита
                if i >= count - 1:
                    return
            
            # После кластера делаем небольшую паузу
            await asyncio.sleep(random.uniform(0.1, 0.5))
        
        else:
            # Обычная транзакция
            # Интервал между транзакциями по экспоненциальному распределению
            delay = random.expovariate(current_tps)
            
            # Добавляем немного джиттера для реалистичности
            jitter = random.uniform(0.95, 1.05)
            delay *= jitter
            
            await asyncio.sleep(delay)
            yield i
        
        # Периодический вывод текущего состояния
        if i % 1000 == 0:
            print(f"Транзакция {i}: TPS={current_tps:.1f}, тренд={trend_direction}")


async def controlled_transaction_generator(count, h, p, target_tps=50):
    """
    Генератор с более точным контролем TPS и реалистичными паттернами
    """
    
    min_tps = 10
    max_tps = 100
    current_target_tps = target_tps
    
    # Параметры для адаптивного управления
    measurement_window = 10.0  # Окно измерения в секундах
    adjustment_factor = 0.1   # Коэффициент корректировки
    
    # Состояние системы
    trend_direction = 0
    trend_strength = 0
    
    start_time = time.time()
    transactions_sent = []
    
    for i in range(count):
        current_time = time.time()
        
        # Очищаем старые записи
        transactions_sent = [t for t in transactions_sent if current_time - t <= measurement_window]
        
        # Рассчитываем текущую производительность
        if len(transactions_sent) > 0:
            window_duration = current_time - transactions_sent[0]
            if window_duration > 0:
                current_actual_tps = len(transactions_sent) / window_duration
            else:
                current_actual_tps = 0
        else:
            current_actual_tps = 0
        
        # Обновляем целевую интенсивность (медленные изменения)
        if random.random() < 0.001:  # Редкие изменения тренда
            trend_direction = random.choice([-1, 0, 1])
            trend_strength = random.uniform(0.5, 2.0)
        
        if trend_direction != 0:
            trend_change = trend_direction * trend_strength * 0.1
            current_target_tps += trend_change
            trend_strength *= 0.999
        
        # Малые флуктуации
        if random.random() < 0.05:
            current_target_tps += random.uniform(-2, 2)
        
        current_target_tps = max(min_tps, min(current_target_tps, max_tps))
        
        # Адаптивная корректировка задержки
        if len(transactions_sent) > 5:  # Достаточно данных для корректировки
            tps_error = current_actual_tps - current_target_tps
            # Корректируем задержку на основе ошибки
            base_delay = 1.0 / current_target_tps
            correction = tps_error * adjustment_factor / current_target_tps
            adaptive_delay = base_delay * (1 + correction)
        else:
            adaptive_delay = 1.0 / current_target_tps
        
        # Добавляем реалистичный джиттер
        jitter = random.uniform(0.8, 1.2)
        actual_delay = adaptive_delay * jitter
        
        # Иногда добавляем микрокластеры (2-4 транзакции подряд)
        if random.random() < 0.03:
            micro_cluster_size = random.randint(2, 4)
            for _ in range(micro_cluster_size):
                await asyncio.sleep(actual_delay * 0.1)  # Быстрые транзакции в кластере
                transactions_sent.append(time.time())
                yield i
                if i >= count - 1:
                    return
        else:
            await asyncio.sleep(actual_delay)
            transactions_sent.append(time.time())
            yield i
        
        # Статистика
        if i % 1000 == 0:
            elapsed = current_time - start_time
            overall_tps = (i + 1) / elapsed if elapsed > 0 else 0
            
            print(f"[{p}|{h}] Транзакция {i}: "
                  f"Целевая={current_target_tps:.1f} TPS, "
                  f"Текущая={current_actual_tps:.1f} TPS ({measurement_window}с), "
                  f"Общая={overall_tps:.1f} TPS, "
                  f"тренд={trend_direction}")

async def advanced_transaction_generator(count, base_tps=50):
    """
    Продвинутый генератор с суточными паттернами и более сложным поведением
    """
    
    min_tps = 5
    max_tps = 1000
    current_tps = base_tps
    
    # Паттерны активности (имитация рабочих часов)
    def get_daily_multiplier():
        hour = time.localtime().tm_hour
        if 9 <= hour <= 17:  # Рабочие часы
            return random.uniform(1.2, 2.0)
        elif 6 <= hour <= 9 or 17 <= hour <= 22:  # Утро/вечер
            return random.uniform(0.8, 1.2)
        else:  # Ночь
            return random.uniform(0.3, 0.7)
    
    # Состояние системы
    system_load = 1.0  # Загрузка системы (влияет на задержки)
    
    for i in range(count):
        # Базовая интенсивность с суточным паттерном
        daily_multiplier = get_daily_multiplier()
        target_tps = base_tps * daily_multiplier
        
        # Плавное изменение к целевой интенсивности
        current_tps += (target_tps - current_tps) * 0.01
        
        # Случайные флуктуации
        if random.random() < 0.05:
            current_tps += random.uniform(-10, 10)
        
        # Имитация нагрузки на систему
        if random.random() < 0.001:  # Редкие всплески нагрузки
            system_load = random.uniform(1.5, 3.0)
            print(f"Всплеск нагрузки: {system_load:.2f}")
        else:
            system_load = max(0.8, system_load * 0.999)  # Постепенное снижение
        
        # Ограничиваем диапазон
        current_tps = max(min_tps, min(current_tps, max_tps))
        
        # Рассчитываем задержку с учетом нагрузки
        effective_tps = current_tps / system_load
        delay = random.expovariate(effective_tps)
        
        # Добавляем сетевой джиттер
        network_jitter = random.uniform(0.001, 0.01)
        delay += network_jitter
        
        await asyncio.sleep(delay)
        yield i
        
        if i % 200 == 0:
            print(f"Транзакция {i}: TPS={current_tps:.1f}, нагрузка={system_load:.2f}, "
                  f"суточный множитель={daily_multiplier:.2f}")

async def simple_transaction_generator(count, base_tps=1000):
    """
    Простой и рабочий генератор транзакций с точным контролем TPS
    """
    
    min_tps = 500
    max_tps = 10000
    current_tps = base_tps
    
    # Состояние для плавных изменений
    trend_direction = 0
    trend_strength = 0
    
    start_time = time.time()
    
    for i in range(count):
        # Медленно изменяем целевую интенсивность
        if random.random() < 0.001:  # 0.1% шанс смены тренда
            trend_direction = random.choice([-1, 0, 1])
            trend_strength = random.uniform(0.5, 2.0)
        
        # Применяем тренд
        if trend_direction != 0:
            current_tps += trend_direction * trend_strength * 0.1
            trend_strength *= 0.999
        
        # Небольшие случайные флуктуации
        if random.random() < 0.05:
            current_tps += random.uniform(-1, 1)
        
        # Ограничиваем диапазон
        current_tps = max(min_tps, min(current_tps, max_tps))
        
        # Точная задержка с минимальным джиттером
        base_delay = 1.0 / current_tps
        jitter = random.uniform(0.95, 1.05)  # Всего ±5%
        delay = base_delay * jitter
        
        await asyncio.sleep(delay)
        yield i
        
        # Статистика каждые 1000 транзакций
        if i % 1000 == 0:
            elapsed = time.time() - start_time
            actual_tps = (i + 1) / elapsed if elapsed > 0 else 0
            print(f"Транзакция {i}: Целевая={current_tps:.1f} TPS, "
                  f"Фактическая={actual_tps:.1f} TPS, тренд={trend_direction}")
            
async def send(t, client, h, p):
    #await asyncio.sleep(random.randint(0, 30))
    """Отправка случайных транзакций в асинхронном режиме."""
    async for _ in controlled_transaction_generator(int(t), h, p):
        
        try:
            pool_response = await client.get(f'{h}/pool')
            pool_data = pool_response.json()
            p = pool_data.get("nodes", [])
            n = len(sk) - 1
            i = random.randint(0, n)
            c = pk[i]  # credit (отправитель)
            priv = sk[i]  # приватный ключ
            d = pk[random.randint(0, n)]  # debit (получатель)
            a = random.randint(1, 1_000_000_000)  # сумма
            u = random.randint(0, 100000)  # уникальный идентификатор
            m = str(random.randint(0, 100000))  # сообщение
            p = pool_data.get("nodes", [])
            # Формируем транзакцию
            tx = dict(
                prnts=p,
                addr=c,
                seq=u,
                var=dict(
                    ca="00000",
                    to=d,
                    val=a,
                    msg=m,
                ),
            )
            m = ordered_sum(tx)  # строка для подписи
            # Подпись транзакции
            s = sign_message(priv, m)
            # Формируем транзакцию

            tx_data = {
                "tx": tx,
                "sign": s,
                "func": "transferToken",
            }
            #logger.info(f"Sending transaction: {json.dumps(tx_data, indent=2)}")
            r = await client.post(f'{h}/', json=tx_data)
            #logger.info(f"Response: {r.text}")
        except Exception as e:
            logger.error(f"Error in send: {e}")

async def run_sender(t, c, p):
    """Запуск C параллельных корутин в одном процессе."""
    HOST = ["http://192.168.1.101:21000", "http://192.168.1.102:21000", "http://192.168.1.103:21000", "http://192.168.1.104:21000", "http://192.168.1.105:21000", "http://192.168.1.106:21000"]
    #HOST = random.choice(["http://127.0.0.1:21001", "http://127.0.0.1:21002", "http://127.0.0.1:21003", "http://127.0.0.1:21004"])
    async with httpx.AsyncClient() as client:
        await asyncio.gather(*[send(t, client, HOST[i], p) for i in range(c)])

def process_target(t, c, p):
    """Функция-цель для каждого процесса."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_sender(t, c, p))
    except KeyboardInterrupt:
        logger.info(f"Process interrupted")
    finally:
        loop.close()

def main():
    args = sys.argv[1:]

    try:
        t = int(args[0])
    except IndexError:
        t = 1_000_000
    try:
        c = int(args[1])
    except IndexError:
        c = 1
    try:
        n = int(args[2])
    except IndexError:
        n = 1

    """Основная функция для запуска многопроцессного генератора транзакций."""
    num_processes = n  # Количество процессов (можно настроить)

    processes = []
    for i in range(num_processes):
        p = Process(target=process_target, args=(t, c, i))
        processes.append(p)
        p.start()

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
    print(time.time() - start)