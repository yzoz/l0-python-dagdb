import math

def log_scaling(n: float, c: float, base: float = math.e) -> float:
    """
    Вычисляет коэффициент k, обратно пропорциональный логарифму N.
    
    Параметры:
        n (float): Входное значение (N >= 1, чтобы избежать log(0)).
        c (float): Масштабирующая константа (C > 0).
        base (float): Основание логарифма (по умолчанию e).
    
    Возвращает:
        float: k = C / log_base(N)
    
    Пример:
        >>> log_scaling(100, 1.0, 10.0)  # 1.0 / log10(100) = 0.5
        0.5
    """
    if n < 1 or c <= 0 or base <= 0:
        raise ValueError("N должно быть >= 1, а C и основание логарифма — положительными.")
    return c / math.log(n, base)

def log_scaling_safe(n: float, c: float, base: float = math.e) -> float | None:
    """Безопасная версия с проверкой ошибок (возвращает None при ошибке)."""
    try:
        return log_scaling(n, c, base)
    except (ValueError, ZeroDivisionError):
        return None

def exp_decay(n: float, c: float, lambda_: float) -> float:
    """
    Вычисляет коэффициент экспоненциального затухания k = C * exp(-λ * N).
    
    Параметры:
        n (float): Входное значение (N >= 0).
        c (float): Начальное значение при N=0 (C > 0).
        lambda_ (float): Коэффициент затухания (λ > 0).
    
    Возвращает:
        float: k = C * exp(-λ * N)
    
    Пример:
        >>> exp_decay(2.0, 1.0, 0.5)  # 1.0 * exp(-0.5 * 2) ≈ 0.367
        0.36787944117144233
    """
    if n < 0 or c <= 0 or lambda_ <= 0:
        raise ValueError("Некорректные параметры: N >= 0, C > 0, λ > 0.")
    return c * math.exp(-lambda_ * n)

def exp_decay_safe(n: float, c: float, lambda_: float) -> float | None:
    """Безопасная версия с обработкой ошибок."""
    try:
        return exp_decay(n, c, lambda_)
    except ValueError:
        return None

def main():
    # Тест обратной пропорциональности логарифму
    n_values = range(1, 25, 1)
    print("Обратная пропорциональность логарифму (C=1.0, base=10.0):")
    for n in n_values:
        k = log_scaling_safe(n, 1)
        print(f"N = {n:>4}, k = {k:.4f}" if k is not None else f"N = {n}: ошибка")

    # Тест экспоненциального затухания
    print("\nЭкспоненциальное затухание (C=1.0, λ=0.1):")
    for n in n_values:
        k = exp_decay_safe(n, 1, 0.01)
        print(f"N = {n:>4}, k = {k:.4f}" if k is not None else f"N = {n}: ошибка")

if __name__ == "__main__":
    main()