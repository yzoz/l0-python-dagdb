import asyncio
import random
import time
import math

async def realistic_transaction_generator(count, base_tps=50):
    """
    Генератор транзакций с реалистичным поведением:
    - Базовая интенсивность с флуктуациями
    - Периодические всплески активности
    - Случайные кластеры транзакций
    - Суточные и недельные паттерны (опционально)
    """
    
    # Параметры системы
    min_tps = 10        # Минимальная интенсивность
    max_tps = 500       # Максимальная интенсивность
    current_tps = base_tps
    
    # Параметры для флуктуаций
    trend_change_prob = 0.005    # Вероятность смены тренда
    small_change_prob = 0.1      # Вероятность малого изменения
    cluster_prob = 0.02          # Вероятность кластера
    
    # Состояние тренда
    trend_direction = 0  # -1, 0, 1
    trend_strength = 0
    
    last_time = time.time()
    start_time = time.time()
    
    # Статистика для измерения реальной производительности
    stats_window = []
    last_stats_time = start_time
    
    for i in range(count):
        current_time = time.time()
        
        # Добавляем текущую транзакцию в статистику
        stats_window.append(current_time)
        
        # Очищаем старые записи (оставляем только за последние 10 секунд)
        stats_window = [t for t in stats_window if current_time - t <= 10.0]
        
        # Обновляем интенсивность только если прошло достаточно времени
        if current_time - last_time > 0.1:  # Каждые 100мс
            # Большие изменения тренда (редко)
            if random.random() < trend_change_prob:
                trend_direction = random.choice([-1, 0, 1])
                trend_strength = random.uniform(0.5, 2.0)
                print(f"Смена тренда: направление={trend_direction}, сила={trend_strength:.2f}")
            
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
            cluster_intensity = current_tps * random.uniform(1.5, 3.0)  # Умеренное увеличение
            
            print(f"Кластер: размер={cluster_size}, интенсивность={cluster_intensity:.1f} tps")
            
            for j in range(cluster_size):
                # Интервал внутри кластера
                cluster_delay = random.expovariate(cluster_intensity)
                await asyncio.sleep(cluster_delay)
                
                # Обновляем статистику для кластера
                cluster_time = time.time()
                stats_window.append(cluster_time)
                stats_window = [t for t in stats_window if cluster_time - t <= 10.0]
                
                yield i
                
                # Прерываем кластер если достигли лимита
                if i >= count - 1:
                    return
            
            # После кластера делаем паузу пропорциональную размеру кластера
            pause_duration = cluster_size / current_tps * random.uniform(0.5, 1.5)
            await asyncio.sleep(pause_duration)
        
        else:
            # Обычная транзакция
            # Интервал между транзакциями по экспоненциальному распределению
            delay = random.expovariate(current_tps)
            
            # Добавляем немного джиттера для реалистичности
            jitter = random.uniform(0.95, 1.05)
            delay *= jitter
            
            await asyncio.sleep(delay)
            yield i
        
        # Периодический вывод текущего состояния с реальной статистикой
        if i % 100 == 0 and len(stats_window) > 1:
            # Рассчитываем реальную производительность
            time_span = stats_window[-1] - stats_window[0]
            real_tps = len(stats_window) / time_span if time_span > 0 else 0
            
            # Общая статистика с начала
            total_time = current_time - start_time
            avg_tps = (i + 1) / total_time if total_time > 0 else 0
            
            print(f"Транзакция {i}: Целевая={current_tps:.1f} TPS, "
                  f"Реальная={real_tps:.1f} TPS (10с), "
                  f"Средняя={avg_tps:.1f} TPS, тренд={trend_direction}")


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


async def controlled_transaction_generator(count, target_tps=50):
    """
    Генератор с более точным контролем TPS и реалистичными паттернами
    """
    
    min_tps = 10
    max_tps = 500
    current_target_tps = target_tps
    
    # Параметры для адаптивного управления
    measurement_window = 5.0  # Окно измерения в секундах
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
        if i % 100 == 0:
            elapsed = current_time - start_time
            overall_tps = (i + 1) / elapsed if elapsed > 0 else 0
            
            print(f"Транзакция {i}: "
                  f"Целевая={current_target_tps:.1f} TPS, "
                  f"Текущая={current_actual_tps:.1f} TPS ({measurement_window}с), "
                  f"Общая={overall_tps:.1f} TPS, "
                  f"тренд={trend_direction}")


# Обновленный пример использования
async def main():
    print("Тестирование генератора транзакций...")
    
    start_time = time.time()
    transaction_count = 0
    
    # Выбираем генератор
    generator_type = "controlled"  # "realistic" или "controlled"
    
    if generator_type == "controlled":
        generator = controlled_transaction_generator(1000, target_tps=100)
    else:
        generator = realistic_transaction_generator(1000, base_tps=100)
    
    async for tx_id in generator:
        transaction_count += 1
        
        # Останавливаемся через 30 секунд для демонстрации
        if time.time() - start_time > 300:
            break
    
    elapsed = time.time() - start_time
    actual_tps = transaction_count / elapsed
    print(f"\nИтоговая статистика:")
    print(f"Транзакций: {transaction_count}")
    print(f"Время: {elapsed:.2f} сек")
    print(f"Фактическая интенсивность: {actual_tps:.2f} TPS")

# Запуск
if __name__ == "__main__":
    asyncio.run(main())