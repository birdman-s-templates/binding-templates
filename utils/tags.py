import allure
import pytest


# Теги Allure
TAG_NAMES = [
    'business_critical',
    'regression',
    'smoke',
    'api',
    'e2e',
]

# Кастомные поля для Allure (labels)
MARKER_SPECS = {
    "author": {
        "allowed": {"Кащеев Денис"},
        "default": "Кащеев Денис",
    },
    "test_type": {
        "allowed": {
            "Full Regress",
            "Новая функциональность",
            "Smart Regress",
            "Интеграционное тестирование",
            "A|B тестирование",
            "Chaos тестирование",
            "Деструктивное тестирование",
        },
        "default": "Новая функциональность",
    },
    "priority": {
        "allowed": {"Критический", "Высокий", "Средний", "Низкий"},
        "default": "Средний",
    },
    "negative": {
        "allowed": {"Негативный"},
        "default": None
    }
}

CUSTOM_LABELS = list(MARKER_SPECS.keys())
__all__ = TAG_NAMES + CUSTOM_LABELS + ["mark_and_tag", "MARKER_SPECS"]

# Декоратор-обёртка
def mark_and_tag(*tags):
    def wrapper(func):
        for tag in tags:
            func = pytest.mark.__getattr__(tag)(allure.tag(tag)(func))
        return func
    return wrapper


# Динамически возвращаем нужные декораторы
def __getattr__(name):
    if name in TAG_NAMES:
        return mark_and_tag(name)
    if name in CUSTOM_LABELS:
        def label_func(value):
            return pytest.mark.__getattr__(name)(value)
        return label_func
    raise AttributeError(f"module 'tags' has no attribute '{name}'")