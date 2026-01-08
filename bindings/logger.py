import logging
import sys
import inspect


class FunctionNameFormatter(logging.Formatter):
    """Кастомный форматтер, который автоматически добавляет имя функции"""

    def format(self, record):
        # Получаем имя функции из стека вызовов
        frame = inspect.currentframe()
        try:
            # Ищем фрейм, который не относится к logging модулю
            while frame:
                frame = frame.f_back
                if frame and not frame.f_code.co_filename.endswith('logging/__init__.py'):
                    # Проверяем, что это не наш Logger класс
                    if 'logger' not in frame.f_code.co_filename.lower():
                        function_name = frame.f_code.co_name
                        record.funcname = function_name
                        break
        finally:
            del frame

        # Если не удалось определить, используем пустую строку
        if not hasattr(record, 'funcname'):
            record.funcname = ''

        return super().format(record)


class Logger:

    def __init__(self, component_name, include_function_name=True):
        self.component_name = component_name
        self.include_function_name = include_function_name
        self.logger = logging.getLogger(component_name)
        self.logger.setLevel(logging.INFO)

        if not self.logger.handlers:
            handler = logging.StreamHandler(sys.stdout)

            if include_function_name:
                formatter = FunctionNameFormatter(
                    f'%(asctime)s - [%(name)s] - %(levelname)s - [%(funcname)s] %(message)s'
                )
            else:
                formatter = logging.Formatter(
                    f'%(asctime)s - [%(name)s] - %(levelname)s - %(message)s'
                )

            handler.setFormatter(formatter)
            self.logger.addHandler(handler)

    def init_logger(self):
        return self.logger