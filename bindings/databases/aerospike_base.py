import datetime
import aerospike
from aerospike import exception as ex
from typing import List, Tuple

import config
from bindings.logger import Logger


class AerospikeClient:
    """
    Класс для Aerospike, позволяющий работать с его объектами с помощью контекстного менеджера with, дабы закрытие
    соединения выполнялось автоматически
    """

    def __init__(self, hosts: List[Tuple[str, int]] = config.AEROSPIKE_HOSTS):
        self.config = {
            'hosts': hosts,
            'policies': {
                'timeout': 60000,  # таймаут сокета
                'total_timeout': 60000,  # общий таймаут для операции
                'max_retries': 3
            }
        }
        self.client = None

        self.logger = Logger(component_name="Aerospike").init_logger()

    def __enter__(self):
        self.client = aerospike.client(self.config).connect()
        self.logger.info(f"Connected to Aerospike: {self.config}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.client:
            self.client.close()
            self.logger.info(f"Connection closed: {self.config}")

    def ensure_connected(self):
        if not self.client.is_connected():
            self.client.connect()
            self.logger.info("Соединение с Aerospike восстановлено")

    @staticmethod
    def get_path(namespace, set_name, key) -> Tuple[str, str, int]:
        return namespace, set_name, key

    def write(self, namespace, set, key, bins, rewrite=True, ttl=None):
        """
        Функция создает запись в Аэроспайке.
            - если rewrite=True, тогда будет выполнена перезапись, а функция вернет True, если rewrite=False и
            перезапись выполнена не будет, вернем False
            - ttl, если параметр задан, но переопределим дэфолтное для сета время жизни записи
        """

        try:
            self.ensure_connected()

            path = self.get_path(namespace, set, key)

            if self.client.exists(path) and not rewrite:
                self.logger.info(f"Запись с ключом {key} уже существует в {namespace}.{set}, перезапись не "
                                 f"выполнялась")
                return False

            elif self.client.exists(path) and rewrite:
                if ttl is not None:
                    self.client.put(path, bins)
                    self.client.touch(path, ttl)
                    self.logger.info(f"Запись с ключом {key} перезаписана в {namespace}.{set} c ttl={ttl}")
                else:
                    self.client.put(path, bins)
                    self.logger.info(f"Запись с ключом {key} перезаписана в {namespace}.{set}")

            else:
                if ttl is not None:
                    self.client.put(path, bins)
                    self.client.touch(path, ttl)
                    self.logger.info(f"Запись с ключом {key} создана в {namespace}.{set}c ttl={ttl}")
                else:
                    self.client.put(path, bins)
                    self.logger.info(f"Запись с ключом {key} создана в {namespace}.{set}")

            return True

        except Exception as e:
            self.logger.error(e)
            raise

    def read(self, namespace, set, key):

        try:
            self.ensure_connected()

            path = self.get_path(namespace, set, key)

            (key, metadata, record) = self.client.get(path)
            # self.logger.info(f"Запись с ключом {key} прочитана в {namespace}.{set}")

            return record

        except ex.RecordNotFound:
            self.logger.debug("Запись не найдена")
            return None

        except Exception as e:
            self.logger.error(e)
            raise

    def get_batch(self, namespace, set, limit):

        records = []
        count = 0

        scan = self.client.scan(namespace, set)

        def scan_callback(record):
            nonlocal count
            if count >= limit:
                return False  # Прерываем сканирование
            records.append(record)
            count += 1

        scan.foreach(scan_callback)
        return records

    def get_all_records(self, namespace, set):
        records = []

        scan = self.client.scan(namespace, set)

        def scan_callback(record):
            records.append(record)

        scan.foreach(scan_callback)
        return records

    def delete(self, namespace, set_name, key):

        try:
            self.ensure_connected()

            path = self.get_path(namespace, set_name, key)

            self.client.remove(path)
            self.logger.info(f"Запись с ключом {key} удалена в {namespace}.{set_name}")

        except ex.RecordNotFound:
            self.logger.info("Запись не найдена")

        except Exception as e:
            self.logger.error(e)
            raise
