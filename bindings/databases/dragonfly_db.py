import redis
from typing import List, Tuple, Optional

import config
from common.bindings.logger import Logger


class Dragonfly:
    """
    Класс использует клиент Redis для взаимодействия с DragonflyDB, так как DragonflyDB совместим с протоколом Redis.

    :param host (str): Хост, на котором запущен сервер DragonflyDB.
    :param port (int): Порт, на котором запущен сервер DragonflyDB.
    :param db (int): Номер базы данных для подключения.
    :param client (redis.Redis): Клиент для подключения к базе данных.
    """

    def __init__(
            self, host: str = None, port: int = None, db=0, connection_timeout: float = 10.0,
            connection_attempts: int = 3, is_shrinker: bool = False, decode_responses: bool = True,
            is_refrigerator_geo: bool = False
    ):
        """
        Инициализация подключения к базе данных DragonflyDB.

        :param host (str, optional): Хост, на котором запущен сервер DragonflyDB. По умолчанию 'localhost'.
        :param port (int, optional): Порт сервера DragonflyDB. По умолчанию 6379.
        :param db (int, optional): Номер базы данных для подключения. По умолчанию 0.
        :param connection_timeout (float, optional): Таймаут подключения в секундах. По умолчанию 10.0.
        :param connection_attempts (int, optional): Количество попыток подключения. По умолчанию 3.
        :param is_shrinker (bool, optional): У нас 2 режима работы - с CP Online и с Shrinker, там разные данные для
        подключения и подходы к хранению данных. Если is_shrinker=True, то из конфига пробросятся нужные данные для
        коннекта, иначе будут использоваться те, что нужны для CP Online. Для Shrinker есть свои группы методов -
        imsi_by_msisdn и msisdn_by_imsi
        """

        if host is None:
            if is_shrinker:
                host = config.DRAGONFLY_HOSTS_SHRINKER
            else:
                host = config.DRAGONFLY_HOSTS
        self.host = host

        if port is None:
            if is_shrinker:
                port = config.DRAGONFLY_PORT_SHRINKER
            elif is_refrigerator_geo:
                port = config.DRAGONFLY_PORT_REFRIGERATOR_GEO
            else:
                port = config.DRAGONFLY_PORT
        self.port = port

        self.db = db
        self.connection_timeout = connection_timeout
        self.connection_attempts = connection_attempts
        self.decode_responses = decode_responses
        self.client = None

        self.logger = Logger(component_name="Dragonfly").init_logger()

    def __enter__(self):
        """
        Устанавливает соединение с базой данных при входе в контекст.
        Проводит несколько попыток подключения, если первоначальная попытка не удалась.
        """
        attempt = 0
        while attempt < self.connection_attempts:
            try:
                self.client = redis.Redis(
                    host=self.host,
                    port=self.port,
                    db=self.db,
                    socket_timeout=self.connection_timeout,
                    decode_responses=self.decode_responses
                )
                self.client.ping()
                break  # Соединение установлено, выходим из цикла
            except Exception as e:
                attempt += 1
                if attempt >= self.connection_attempts:
                    raise ConnectionError(
                        f"Не удалось подключиться к DragonflyDB после {self.connection_attempts} попыток: {e}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Закрывает соединение с базой данных при выходе из контекста.

        :param exc_type: Тип исключения, если оно возникло.
        :param exc_val: Значение исключения, если оно возникло.
        :param exc_tb: Трассировка исключения, если оно возникло.
        """
        if self.client:
            self.client.close()
        if exc_type:
            self.logger.warning(f"Исключение {exc_type}: {exc_val}")

    def create(self, key: str, value: str) -> bool:
        """
        Создаёт или обновляет запись в базе данных.

        :param key: Ключ записи.
        :param value: Значение записи.
        :return: True, если операция прошла успешно, иначе False.
        """
        try:
            return self.client.set(key, value)
        except Exception as e:
            self.logger.warning(f"Ошибка при создании/обновлении записи '{key}': {e}")
            return False

    def read(self, key: str):
        """
        Читает значение записи по заданному ключу, обрабатывая различные типы данных.

        :param key: Ключ записи.
        :return: Значение записи в зависимости от её типа или None, если ключ не существует.
        """
        try:
            key_type = self.client.type(key)
            if key_type == 'none':
                return None
            elif key_type == 'string':
                return self.client.get(key)
            elif key_type == 'hash':
                return self.client.hgetall(key)
            elif key_type == 'list':
                return self.client.lrange(key, 0, -1)
            elif key_type == 'set':
                return list(self.client.smembers(key))
            elif key_type == 'zset':
                return self.client.zrange(key, 0, -1)
            else:
                self.logger.warning(f"Неизвестный или неподдерживаемый тип данных: {key_type}")
                return None
        except Exception as e:
            self.logger.warning(f"Ошибка при чтении записи '{key}': {e}")
            return None

    def get_hash_by_key(self, hash_key: str) -> dict | None:
        """
        Получает hash с проверкой типа и декодированием.
        """
        try:
            key_type = self.client.type(hash_key)
            if key_type != 'hash':
                return None

            raw_data = self.client.hgetall(hash_key)
            if not raw_data:
                return None

            # Декодирование bytes
            if raw_data and isinstance(next(iter(raw_data)), bytes):
                return {k.decode('utf-8'): v.decode('utf-8') for k, v in raw_data.items()}
            return dict(raw_data)

        except Exception as e:
            self.logger.warning(f"Ошибка при получении hash '{hash_key}': {e}")
            return None

    def get_field_by_key(self, key_type: str, key_value: str, field: str):
        """
        Универсальный метод для получения значения поля из хэша.

        :param key_type: Тип ключа (imsi, msisdn, imei, guid, vlr)
        :param key_value: Значение ключа
        :param field: Имя поля для получения
        :return: Значение поля или None
        """
        key = f"{key_type}:{key_value}"
        try:
            result = self.client.hget(key, field)
            return result
        except Exception as e:
            self.logger.warning(f"Ошибка при получении {field} по {key_type} '{key_value}': {e}")
            return None

    def read_n_records(self, n: int) -> dict:
        """
        Читает заданное количество записей из базы данных, используя команду SCAN.

        :param n: Количество записей для возврата.
        :return: Словарь с ключами и их значениями.
        """
        records = {}
        cursor = 0

        try:
            while len(records) < n:
                cursor, keys = self.client.scan(cursor=cursor, count=n)
                for key in keys:
                    records[key] = self.read(key)
                    if len(records) >= n:
                        break
                if cursor == 0:
                    break
        except Exception as e:
            self.logger.warning(f"Ошибка при чтении записей: {e}")

        return records

    def get_all_keys(self, pattern: str = '*', limit: Optional[int] = None) -> List[str]:
        """
        Возвращает список ключей, удовлетворяющих заданному шаблону, с ограничением количества ключей.

        Используется итератор scan_iter для последовательного получения ключей.

        :param pattern: Шаблон для фильтрации ключей (по умолчанию '*' – все ключи).
        :param limit: Максимальное количество ключей, которые необходимо вернуть. Если None, выбираются все подходящие ключи.
        :return: Список ключей, удовлетворяющих шаблону.
        """
        keys = []
        try:
            for key in self.client.scan_iter(match=pattern):
                keys.append(key)
                if limit is not None and len(keys) >= limit:
                    break
        except Exception as e:
            self.logger.warning(f"Ошибка при получении ключей с шаблоном '{pattern}': {e}")
        return keys

    def update(self, key: str, value: str) -> bool:
        """
        Обновляет значение записи по заданному ключу.

        :param key: Ключ записи.
        :param value: Новое значение записи.
        :return: True, если операция прошла успешно, иначе False.
        """
        return self.create(key, value)

    def delete(self, key: str) -> bool:
        """
        Удаляет запись из базы данных по заданному ключу.

        :param key: Ключ записи.
        :return: True, если запись была успешно удалена, иначе False.
        """
        try:
            return self.client.delete(key) == 1
        except Exception as e:
            self.logger.warning(f"Ошибка при удалении записи '{key}': {e}")
            return False

    def set_field_by_key(self, key_type: str, key_value: str, field: str, field_value: str) -> bool:
        """
        Универсальный метод для установки значения поля в хэше.

        :param key_type: Тип ключа (imsi, msisdn, imei, guid, vlr)
        :param key_value: Значение ключа
        :param field: Имя поля
        :param field_value: Значение поля
        :return: True если успешно, иначе False
        """
        key = f"{key_type}:{key_value}"
        try:
            self.client.hset(key, field, field_value)
            return True
        except Exception as e:
            self.logger.warning(f"Ошибка при установке {field} для {key_type} '{key_value}': {e}")
            return False

    def delete_key(self, key_type: str, key_value: str) -> bool:
        """
        Универсальный метод для удаления ключа.

        :param key_type: Тип ключа (imsi, msisdn, imei, guid, vlr)
        :param key_value: Значение ключа
        :return: True если успешно, иначе False
        """
        key = f"{key_type}:{key_value}"
        try:
            self.client.delete(key)
            return True
        except Exception as e:
            self.logger.warning(f"Ошибка удаления записи для {key_type} '{key_value}': {e}")
            return False

    # Теперь все ваши методы можно заменить на:

    def get_msisdn_by_imsi(self, imsi: str):
        """Получает значение msisdn по заданному imsi."""
        return self.get_field_by_key("imsi", imsi, "msisdn")

    def get_imsi_by_msisdn(self, msisdn: str):
        """Получает значение imsi по заданному msisdn."""
        return self.get_field_by_key("msisdn", msisdn, "imsi")

    def get_imsi_by_imei(self, imei: str):
        """Получает значение imsi по заданному imei."""
        return self.get_field_by_key("imei", imei, "imsi")

    def get_guid_by_msisdn(self, msisdn: str):
        """Получает значение guid по заданному msisdn."""
        return self.get_field_by_key("msisdn", msisdn, "guid")

    def get_msisdn_by_guid(self, guid: str):
        """Получает значение msisdn по заданному guid."""
        return self.get_field_by_key("guid", guid, "msisdn")

    def create_imsi_by_imei(self, imei: str, imsi: str) -> bool:
        """Создает или обновляет хэш imei -> imsi."""
        return self.set_field_by_key("imei", imei, "imsi", imsi)

    def create_msisdn_by_imsi(self, imsi: str, msisdn: str) -> bool:
        """Создает или обновляет хэш imsi -> msisdn."""
        return self.set_field_by_key("imsi", imsi, "msisdn", msisdn)

    def create_imsi_by_msisdn(self, msisdn: str, imsi: str) -> bool:
        """Создает или обновляет хэш msisdn -> imsi."""
        return self.set_field_by_key("msisdn", msisdn, "imsi", imsi)

    def create_guid_by_msisdn(self, msisdn: str, guid: str) -> bool:
        """Создает или обновляет хэш msisdn -> guid."""
        return self.set_field_by_key("msisdn", msisdn, "guid", guid)

    def create_msisdn_by_guid(self, guid: str, msisdn: str) -> bool:
        """Создает или обновляет хэш guid -> msisdn."""
        return self.set_field_by_key("guid", guid, "msisdn", msisdn)

    def update_msisdn_by_imsi(self, imsi: str, new_msisdn: str) -> bool:
        """Обновляет значение msisdn для заданного imsi."""
        return self.set_field_by_key("imsi", imsi, "msisdn", new_msisdn)

    def update_imsi_by_msisdn(self, msisdn: str, new_imsi: str) -> bool:
        """Обновляет значение imsi для заданного msisdn."""
        return self.set_field_by_key("msisdn", msisdn, "imsi", new_imsi)

    def delete_msisdn_by_imsi(self, imsi: str) -> bool:
        """Удаляет хэш-запись по ключу imsi."""
        return self.delete_key("imsi", imsi)

    def delete_imsi_by_msisdn(self, msisdn: str) -> bool:
        """Удаляет хэш-запись по ключу msisdn."""
        return self.delete_key("msisdn", msisdn)

    # Для VLR методов - отдельная реализация, так как там другая логика:

    def create_vlr_pair(self, prefix: str, country_name: str, meta_country_name: str, operator_name: str) -> bool:
        """Создает VLR запись с несколькими полями."""
        key = f"vlr:{prefix}"
        try:
            mapping = {
                "countryName": country_name,
                "metaCountryName": meta_country_name,
                "operatorName": operator_name
            }
            self.client.hset(key, mapping=mapping)
            return True
        except Exception as e:
            self.logger.warning(f"Ошибка при создании записи для vlr '{prefix}': {e}")
            return False

    def get_vlr_pair(self, prefix: str):
        """Получает VLR запись."""
        return self.get_hash_by_key(f"vlr:{prefix}")

    def delete_vlr_pair(self, prefix: str) -> bool:
        """Удаляет VLR запись."""
        return self.delete_key("vlr", prefix)

    def delete_timestamp_by_imsi(self, imsi: str):
        """Удаляет timestamp по imsi (для тестов)."""
        try:
            result = self.client.delete(imsi)
            return result
        except Exception as e:
            self.logger.warning(f"Ошибка при удалении imsi '{imsi}': {e}")
            return None

    def create_records_batch(self, users_data: List[Tuple[str, dict]]) -> bool:
        """
        Создает записи для пользователей в батч-режиме.

        :param users_data: Список словарей с данными пользователей
        :return: True, если все операции прошли успешно, иначе False
        """
        try:
            # Используем pipeline для батчевых операций
            pipe = self.client.pipeline()

            for user in users_data:
                key, data = user
                try:
                    pipe.hset(key, mapping=data)
                except:
                    self.logger.warning(f"Ошибка при создании записей пользователя: {key}, {data}")
                    return False

            # Выполняем все операции одним батчем
            pipe.execute()
            self.logger.info(f"Успешно создано записей для {len(users_data)} пользователей")
            return True

        except Exception as e:
            self.logger.warning(f"Ошибка при создании записей пользователей: {e}")
            return False

    def delete_all_records_by_pattern(self) -> bool:
        """
        Удаляет ВСЕ записи пользователей по паттернам msisdn:*, imsi:*, guid:*.

        :return: True, если операция прошла успешно, иначе False
        """
        try:
            patterns = ['msisdn:*', 'imsi:*', 'guid:*']
            total_deleted = 0

            for pattern in patterns:
                # Получаем все ключи по паттерну
                keys = []
                for key in self.client.scan_iter(match=pattern):
                    keys.append(key)

                if keys:
                    # Удаляем ключи батчами по 1000 штук
                    batch_size = 1000
                    for i in range(0, len(keys), batch_size):
                        batch_keys = keys[i:i + batch_size]
                        deleted = self.client.delete(*batch_keys)
                        total_deleted += deleted

            self.logger.info(f"Удалено {total_deleted} записей пользователей")
            return True

        except Exception as e:
            self.logger.warning(f"Ошибка при удалении всех записей пользователей: {e}")
            return False
