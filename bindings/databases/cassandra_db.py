from __future__ import annotations
import time
from cassandra import ConsistencyLevel
from cassandra.auth import PlainTextAuthProvider
from cassandra.cluster import Cluster, ExecutionProfile, EXEC_PROFILE_DEFAULT
from cassandra.query import BatchStatement, BatchType, SimpleStatement
from cassandra.policies import TokenAwarePolicy, DCAwareRoundRobinPolicy
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import List, Optional, Dict, Tuple, Any

import config
from bindings.logger import Logger
from utils.state import State


class Cassandra:

    _instance_counter = 0

    def __init__(self, contact_points: List[str] = None, port: int = None, keyspace: str = 'data_river',
                 username: Optional[str] = None, password: Optional[str] = None,
                 connection_timeout: float = 10.0, connection_attempts: int = 5, request_timeout: int = 20,
                 local_dc: Optional[str] = None
) -> None:
        """
        Инициализация подключения к Cassandra.
        :param contact_points: Список IP-адресов или доменных имен узлов кластера.
        :param port: Порт подключения к Cassandra (по умолчанию 9042 или значение из config.CASSANDRA_PORT).
        :param keyspace: Название keyspace (аналог базы данных в Cassandra).
        :param username: Логин для аутентификации (если требуется).
        :param password: Пароль для аутентификации (если требуется).
        :param connection_timeout: Таймаут подключения в секундах.
        :param connection_attempts: Количество попыток подключения.
        """
        if contact_points is None:
            contact_points = config.CASSANDRA_HOSTS
        self.contact_points = contact_points

        if port is None:
            port = config.CASSANDRA_PORT
        self.port = port

        if local_dc is None:
            local_dc = config.CASSANDRA_DC
        self.local_dc = local_dc

        self.keyspace = keyspace

        if username is None or password is None:

            if config.env == 'prep1':
                username = State.prep_user
                password = State.prep_password

            if config.env == 'test1':
                username = State.test_user
                password = State.test_password

        self.username = username
        self.password = password

        self.request_timeout = request_timeout
        self.connection_timeout = connection_timeout
        self.connection_attempts = connection_attempts

        self.cluster = None
        self.session = None

        Cassandra._instance_counter += 1
        self.instance_number = Cassandra._instance_counter

        self.logger = Logger(component_name="Cassandra").init_logger()

        self.logger.info(
            f"[Экземпляр #{self.instance_number}], "
            f"contact_points: {self.contact_points},"
            f"port: {self.port},"
            f"local_dc: {self.local_dc},"
            f"keyspace: {self.keyspace},"
            f"username: {self.username},"
        )

    def __enter__(self) -> "Cassandra":
        """
        Устанавливает соединение с Cassandra при входе в контекстный менеджер.
        Проводит несколько попыток подключения, если первоначальная попытка не удалась.
        :return: Экземпляр класса Cassandra.
        """
        attempt = 0
        while attempt < self.connection_attempts:
            try:
                auth_provider = PlainTextAuthProvider(self.username,
                                                      self.password) if self.username and self.password else None

                # Создаем политику для работы с датацентрами
                lb_policy = TokenAwarePolicy(  # Комбинированная политика:
                    DCAwareRoundRobinPolicy(local_dc=self.local_dc)  # Указываем целевой датацентр
                )

                # Создаем профиль выполнения
                profile = ExecutionProfile(
                    load_balancing_policy=lb_policy,
                    request_timeout=self.request_timeout,
                    consistency_level=ConsistencyLevel.LOCAL_QUORUM  # Пример уровня согласованности
                )

                self.cluster = Cluster(
                    self.contact_points,
                    port=self.port,
                    auth_provider=auth_provider,
                    connect_timeout=self.connection_timeout,
                    execution_profiles={
                        EXEC_PROFILE_DEFAULT: profile  # Используем дефолтный профиль
                    }
                )
                self.session = self.cluster.connect()
                self.session.set_keyspace(self.keyspace)
                break  # Подключение успешно установлено
            except Exception as e:
                attempt += 1
                if attempt >= self.connection_attempts:
                    raise ConnectionError(
                        f"Не удалось подключиться к Cassandra после {self.connection_attempts} попыток: {e}")
        return self

    def __exit__(self, exc_type: Optional[type], exc_value: Optional[BaseException], traceback: Optional[Any]) -> None:
        """
        Закрывает соединение при выходе из контекстного менеджера.
        """
        self.close()

    def insert(self, table_name: str, columns: List[str], values: tuple) -> None:
        """
        Вставляет данные в таблицу с использованием подготовленного запроса.
        :param table_name: Название таблицы.
        :param columns: Список колонок для вставки.
        :param values: Кортеж значений для вставки.
        """
        placeholders = ', '.join(['?'] * len(columns))
        query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders});"
        prepared_stmt = self.session.prepare(query)
        self.session.execute(prepared_stmt, values)

    def insert_dict_to_cassandra(self, table_name, data_dict):
        """
        Универсальная функция для записи словаря в таблицу Cassandra
        """
        # Формирование запроса динамически
        columns = ', '.join(data_dict.keys())
        placeholders = ', '.join(['?' for _ in data_dict])

        insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({placeholders})"

        prepared = self.session.prepare(insert_query)
        self.session.execute(prepared, list(data_dict.values()))

    def select(self, table_name: str, columns: str = '*', condition: Optional[str] = None) -> Any:
        """
        Выбирает данные из таблицы.
        :param table_name: Название таблицы.
        :param columns: Столбцы для выборки (по умолчанию '*').
        :param condition: Условие WHERE (например, 'id = 12345').
        :return: Результат выполнения запроса.
        """
        query = f"SELECT {columns} FROM {table_name}"
        if condition:
            query += f" WHERE {condition}"
        query += ';'
        return self.session.execute(query)

    def get_all_keys(self, table_name: str, limit: Optional[int] = None) -> List[Tuple]:
        """
        Возвращает список значений первичных ключей из указанной таблицы с ограничением количества строк.

        Метод извлекает имена столбцов первичного ключа (partition и clustering key)
        из метаданных кластера Cassandra, затем выполняет запрос для выборки этих столбцов.

        :param table_name: Название таблицы.
        :param limit: Максимальное количество строк для выборки. Если None, выбираются все строки.
        :return: Список кортежей с ключами каждой строки.
        """
        # Получаем метаданные таблицы из выбранного keyspace
        table_metadata = self.cluster.metadata.keyspaces[self.keyspace].tables.get(table_name)
        if table_metadata is None:
            raise ValueError(f"Таблица {table_name} не найдена в keyspace {self.keyspace}.")

        # Формируем список имён столбцов первичного ключа.
        partition_keys = [col.name for col in table_metadata.partition_key]
        clustering_keys = [col.name for col in table_metadata.clustering_key]
        primary_key_columns = partition_keys + clustering_keys

        # Строим SQL-запрос для выборки ключей, с учётом лимита
        columns_str = ', '.join(primary_key_columns)
        query = f"SELECT {columns_str} FROM {table_name}"
        if limit is not None:
            query += f" LIMIT {limit}"
        query += ';'

        result = self.session.execute(query)
        return list(result)

    def update(self, table_name: str, updates: Dict[str, Any], condition: str) -> None:
        """
        Обновляет данные в таблице.
        :param table_name: Название таблицы.
        :param updates: Словарь {"колонка": новое значение} для обновления.
        :param condition: Условие WHERE.
        """
        set_clause = ', '.join([f"{col} = ?" for col in updates.keys()])
        values = tuple(updates.values())
        query = f"UPDATE {table_name} SET {set_clause} WHERE {condition};"
        prepared_stmt = self.session.prepare(query)
        self.session.execute(prepared_stmt, values)

    def delete(self, table_name: str, condition: str) -> None:
        """
        Удаляет данные из таблицы.
        :param table_name: Название таблицы.
        :param condition: Условие WHERE.
        """
        query = f"DELETE FROM {table_name} WHERE {condition};"
        self.session.execute(query)

    def truncate(self, table_name: str, timeout: int = None) -> None:
        """
        Удаляет все данные из таблицы.
        :param table_name: Название таблицы.
        """
        query = f"TRUNCATE TABLE {table_name};"

        if timeout is None:
            self.session.execute(query)
        else:
            self.session.execute(query, timeout=timeout)

    def close(self) -> None:
        """
        Закрывает соединение с базой данных.
        """
        if self.session:
            self.session.shutdown()
        if self.cluster:
            self.cluster.shutdown()

    def delete_all_rows(
            self,
            table_name: str,
            page_size: int = 1000,
            concurrency: int = 20,
            retry_delay: float = 0.5,
            max_retries: int = 3
    ) -> None:
        """
        Удаляет все записи из таблицы построчно с пагинацией и управлением параллелизмом.

        :param table_name: Название таблицы для очистки
        :param page_size: Количество строк для обработки за одну итерацию (пагинация)
        :param concurrency: Максимальное количество одновременных DELETE-запросов
        :param retry_delay: Задержка между повторными попытками при ошибках (в секундах)
        :param max_retries: Максимальное количество попыток для одного запроса

        Особенности:
        - Использует пагинацию для обработки больших таблиц
        - Контролирует нагрузку через управление параллелизмом
        - Автоматически повторяет запросы при временных ошибках
        - Логирует прогресс выполнения
        """
        # Получаем метаданные таблицы
        table_metadata = self.cluster.metadata.keyspaces[self.keyspace].tables.get(table_name)
        if table_metadata is None:
            raise ValueError(f"Таблица {table_name} не найдена в keyspace {self.keyspace}.")

        # Формируем список колонок первичного ключа
        pk_columns = [
                         col.name for col in table_metadata.partition_key
                     ] + [
                         col.name for col in table_metadata.clustering_key
                     ]

        if not pk_columns:
            raise ValueError(f"Не удалось определить первичный ключ для таблицы {table_name}")

        # Подготавливаем запрос для получения первичных ключей
        columns_str = ', '.join(pk_columns)
        select_query = f"SELECT {columns_str} FROM {table_name}"
        statement = SimpleStatement(select_query, fetch_size=page_size)

        # Переменные для отслеживания прогресса
        total_deleted = 0
        page_num = 0
        start_time = time.time()

        self.logger.info(f"Начинаю удаление записей из таблицы {table_name}...")
        self.logger.info(f"Параметры: page_size={page_size}, concurrency={concurrency}")

        try:
            # Получаем первую страницу результатов
            result_set = self.session.execute(statement)

            while True:
                page_num += 1
                rows = list(result_set.current_rows)
                if not rows:
                    break

                self.logger.info(f"Обрабатываю страницу {page_num} ({len(rows)} записей)")

                # Обрабатываем записи текущей страницы пакетами
                for i in range(0, len(rows), concurrency):
                    batch = rows[i:i + concurrency]
                    futures = []

                    # Отправляем асинхронные DELETE-запросы
                    for row in batch:
                        conditions = [f"{col} = ?" for col in pk_columns]
                        where_clause = " AND ".join(conditions)
                        delete_query = f"DELETE FROM {table_name} WHERE {where_clause}"

                        try:
                            prepared = self.session.prepare(delete_query)
                            values = tuple(getattr(row, col) for col in pk_columns)

                            # Повторяем запрос при ошибках
                            for attempt in range(max_retries):
                                try:
                                    future = self.session.execute_async(prepared, values)
                                    futures.append(future)
                                    break
                                except Exception as e:
                                    if attempt == max_retries - 1:
                                        raise
                                    self.logger.info(f"Попытка {attempt + 1}/{max_retries} не удалась: {str(e)}")
                                    time.sleep(retry_delay * (attempt + 1))
                        except Exception as e:
                            self.logger.error(f"Ошибка подготовки запроса для строки: {e}")
                            continue

                    # Ждем завершения всех запросов в пакете
                    for future in futures:
                        try:
                            future.result()
                        except Exception as e:
                            # Логируем ошибку, но продолжаем выполнение
                            self.logger.error(f"Ошибка при удалении записи: {str(e)}")

                    total_deleted += len(batch)
                    elapsed = time.time() - start_time
                    avg_speed = total_deleted / elapsed if elapsed > 0 else 0
                    self.logger.info(f"Удалено записей: {total_deleted} | Скорость: {avg_speed:.1f} записей/сек")

                # Переходим к следующей странице
                if result_set.has_more_pages:
                    result_set = self.session.execute(
                        statement,
                        paging_state=result_set.paging_state
                    )
                else:
                    break

        except Exception as e:
            self.logger.error(f"Критическая ошибка во время удаления: {str(e)}")
            raise
        finally:
            elapsed = time.time() - start_time
            self.logger.info(f"Итоги удаления из таблицы {table_name}: обработано записей: {total_deleted}, "
                             f"время выполнения: {elapsed:.2f} сек")

    def batch_insert_dicts_to_cassandra(self,
                                        records: List[Tuple[str, Dict]],
                                        batch_size: int = 100,
                                        concurrency: int = 10,
                                        retry_delay: float = 0.5,
                                        max_retries: int = 3,
                                        use_prepared: bool = True,
                                        consistency_level: Optional[ConsistencyLevel] = None,
                                        timeout: Optional[float] = None,
                                        log_progress: bool = True) -> Dict[str, Any]:
        """
        Выполняет батчевую вставку записей в разные таблицы с оптимизацией по группировке.

        :param records: Список кортежей (table_name, data_dict)
        :param batch_size: Размер батча для каждой таблицы
        :param concurrency: Количество одновременно выполняемых батчей
        :param retry_delay: Задержка между повторными попытками при ошибках
        :param max_retries: Максимальное количество попыток для одного батча
        :param use_prepared: Использовать подготовленные запросы
        :param consistency_level: Уровень согласованности
        :param timeout: Таймаут для выполнения запроса
        :param log_progress: Логировать прогресс выполнения
        :return: Словарь со статистикой выполнения по таблицам
        """

        if not records:
            self.logger.warning("Передан пустой список записей для вставки")
            return {'total_records': 0, 'tables': {}, 'execution_time': 0}

        # Инициализируем переменные в начале
        start_time = time.time()
        total_records = len(records)
        table_stats = defaultdict(lambda: {'successful': 0, 'failed': 0, 'batches': 0, 'errors': []})
        prepared_statements = {}
        tables_data = defaultdict(list)
        all_tasks = []

        if log_progress:
            self.logger.info(f"Начинаю батчевую вставку {total_records} записей")
            self.logger.info(f"Параметры: batch_size={batch_size}, concurrency={concurrency}")

        try:
            # Группируем записи по таблицам
            for table_name, data_dict in records:
                tables_data[table_name].append(data_dict)

            if log_progress:
                table_stats_preview = {table: len(data) for table, data in tables_data.items()}
                self.logger.info(f"Записи сгруппированы по таблицам: {table_stats_preview}")

            def get_prepared_statement(table_name: str, columns: List[str]) -> Any:
                """Получает или создает подготовленный запрос для таблицы с определенными колонками"""
                columns_key = tuple(sorted(columns))
                cache_key = (table_name, columns_key)

                if cache_key not in prepared_statements:
                    columns_str = ', '.join(columns)
                    placeholders = ', '.join(['?' for _ in columns])
                    insert_query = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"

                    if use_prepared:
                        stmt = self.session.prepare(insert_query)
                        if consistency_level:
                            stmt.consistency_level = consistency_level
                        prepared_statements[cache_key] = stmt
                    else:
                        prepared_statements[cache_key] = insert_query

                return prepared_statements[cache_key]

            def execute_table_batch(table_name: str, batch_data: List[Dict], batch_num: int) -> Tuple[
                str, int, int, List[str]]:
                """
                Выполняет один батч для конкретной таблицы.
                Возвращает (table_name, успешные_записи, неудачные_записи, список_ошибок)
                """
                local_successful = 0
                local_failed = 0
                local_errors = []

                for attempt in range(max_retries):
                    try:
                        # Группируем записи по структуре колонок
                        columns_groups = defaultdict(list)
                        for record in batch_data:
                            columns_key = tuple(sorted(record.keys()))
                            columns_groups[columns_key].append(record)

                        # Обрабатываем каждую группу колонок отдельно
                        for columns_key, group_records in columns_groups.items():
                            columns = list(columns_key)

                            # Создаем batch statement
                            batch_stmt = BatchStatement(batch_type=BatchType.UNLOGGED)
                            if consistency_level:
                                batch_stmt.consistency_level = consistency_level

                            # Получаем подготовленный запрос
                            prepared_stmt = get_prepared_statement(table_name, columns)

                            # Добавляем записи в батч
                            for record in group_records:
                                values = tuple(record[col] for col in columns)

                                if use_prepared:
                                    batch_stmt.add(prepared_stmt, values)
                                else:
                                    from cassandra.query import SimpleStatement
                                    batch_stmt.add(SimpleStatement(prepared_stmt), values)

                            # Выполняем батч для этой группы колонок
                            if timeout:
                                self.session.execute(batch_stmt, timeout=timeout)
                            else:
                                self.session.execute(batch_stmt)

                        local_successful = len(batch_data)
                        break  # Успешно выполнено

                    except Exception as e:
                        error_msg = f"Таблица {table_name}, батч {batch_num}, попытка {attempt + 1}: {str(e)}"
                        local_errors.append(error_msg)

                        if attempt == max_retries - 1:
                            local_failed = len(batch_data)
                            if log_progress:
                                self.logger.error(
                                    f"Батч {batch_num} для таблицы {table_name} не удалось выполнить: {e}")
                        else:
                            time.sleep(retry_delay * (attempt + 1))

                return table_name, local_successful, local_failed, local_errors

            # Создаем задачи для каждой таблицы
            for table_name, table_data in tables_data.items():
                # Разбиваем данные таблицы на батчи
                for i in range(0, len(table_data), batch_size):
                    batch_data = table_data[i:i + batch_size]
                    batch_num = i // batch_size + 1
                    all_tasks.append((table_name, batch_data, batch_num))

            if log_progress:
                self.logger.info(f"Создано {len(all_tasks)} батчей для {len(tables_data)} таблиц")

            # Выполняем все батчи с контролем параллелизма
            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                future_to_task = {
                    executor.submit(execute_table_batch, table_name, batch_data, batch_num): (table_name, batch_data,
                                                                                              batch_num)
                    for table_name, batch_data, batch_num in all_tasks
                }

                completed_tasks = 0

                for future in as_completed(future_to_task):
                    task_table, task_data, task_batch_num = future_to_task[future]

                    try:
                        table_name, local_successful, local_failed, local_errors = future.result()

                        # Обновляем статистику по таблице
                        table_stats[table_name]['successful'] += local_successful
                        table_stats[table_name]['failed'] += local_failed
                        table_stats[table_name]['batches'] += 1
                        table_stats[table_name]['errors'].extend(local_errors)

                        completed_tasks += 1

                        if log_progress and completed_tasks % 10 == 0:
                            elapsed = time.time() - start_time
                            progress = (completed_tasks / len(all_tasks)) * 100
                            total_successful = sum(stats['successful'] for stats in table_stats.values())
                            avg_speed = total_successful / elapsed if elapsed > 0 else 0

                            self.logger.info(
                                f"Прогресс: {completed_tasks}/{len(all_tasks)} батчей ({progress:.1f}%) | "
                                f"Успешно: {total_successful} | "
                                f"Скорость: {avg_speed:.1f} записей/сек"
                            )

                    except Exception as e:
                        self.logger.error(f"Ошибка при обработке батча для таблицы {task_table}: {e}")
                        table_stats[task_table]['failed'] += len(task_data)

        except Exception as e:
            self.logger.error(f"Критическая ошибка во время батчевой вставки: {e}")
            raise

        # Вычисляем финальную статистику (вынесено из finally для избежания недостижимого кода)
        execution_time = time.time() - start_time
        total_successful = sum(stats['successful'] for stats in table_stats.values())
        total_failed = sum(stats['failed'] for stats in table_stats.values())

        final_stats = {
            'total_records': total_records,
            'total_successful': total_successful,
            'total_failed': total_failed,
            'success_rate': (total_successful / total_records * 100) if total_records > 0 else 0,
            'execution_time': execution_time,
            'records_per_second': total_successful / execution_time if execution_time > 0 else 0,
            'tables': dict(table_stats)
        }

        if log_progress:
            self.logger.info(
                f"Батчевая вставка завершена: "
                f"Всего записей: {total_records}. "
                f"Успешно: {total_successful}. "
                f"Ошибок: {total_failed}. "
                f"Время выполнения: {execution_time:.2f} сек."
            )

            # Статистика по таблицам
            for table_name, stats in table_stats.items():
                self.logger.info(f"  Таблица {table_name}: {stats['successful']} успешно, "
                                 f"{stats['failed']} ошибок, {stats['batches']} батчей")

        return final_stats

    def insert_dict_to_cassandra_batch(self, table_name: str, data_dicts: List[Dict], **kwargs):
        """
        Упрощенный метод для батчевой вставки в одну таблицу.
        Адаптер для batch_insert_dicts_to_cassandra для случая одной таблицы.
        """
        records = [(table_name, data_dict) for data_dict in data_dicts]
        return self.batch_insert_dicts_to_cassandra(records, **kwargs)

    def delete_all_rows_batch_multiple(self,
                                       table_names: List[str],
                                       page_size: int = 1000,
                                       batch_size: int = 100,
                                       concurrency: int = 10,
                                       retry_delay: float = 0.5,
                                       max_retries: int = 3) -> Dict[str, int]:
        """
        Удаляет все записи из нескольких таблиц используя batch statements.
        """
        results = {}

        # Подготавливаем метаданные для всех таблиц
        tables_metadata = {}
        prepared_deletes = {}

        for table_name in table_names:
            # Получаем метаданные таблицы
            table_metadata = self.cluster.metadata.keyspaces[self.keyspace].tables.get(table_name)
            if table_metadata is None:
                self.logger.warning(f"Таблица {table_name} не найдена в keyspace {self.keyspace}")
                continue

            tables_metadata[table_name] = table_metadata

            # Подготавливаем DELETE запрос
            pk_columns = [col.name for col in table_metadata.partition_key] + \
                         [col.name for col in table_metadata.clustering_key]
            conditions = [f"{col} = ?" for col in pk_columns]
            where_clause = " AND ".join(conditions)
            delete_query = f"DELETE FROM {table_name} WHERE {where_clause}"
            prepared_deletes[table_name] = {
                'statement': self.session.prepare(delete_query),
                'pk_columns': pk_columns
            }

        def process_table(table_name):
            """Обрабатывает одну таблицу"""
            if table_name not in tables_metadata:
                return table_name, 0

            table_metadata = tables_metadata[table_name]
            prepared_delete_info = prepared_deletes[table_name]
            prepared_delete = prepared_delete_info['statement']
            pk_columns = prepared_delete_info['pk_columns']

            columns_str = ', '.join(pk_columns)
            select_query = f"SELECT {columns_str} FROM {table_name}"
            statement = SimpleStatement(select_query, fetch_size=page_size)

            total_deleted = 0
            start_time = time.time()

            self.logger.info(f"Начинаю удаление записей из таблицы {table_name}")

            def execute_delete_batch(rows_batch, batch_num):
                """Выполняет удаление одного батча для конкретной таблицы"""
                for attempt in range(max_retries):
                    try:
                        batch_stmt = BatchStatement(batch_type=BatchType.UNLOGGED)

                        for row in rows_batch:
                            values = tuple(getattr(row, col) for col in pk_columns)
                            batch_stmt.add(prepared_delete, values)

                        self.session.execute(batch_stmt)
                        return len(rows_batch), 0

                    except Exception as e:
                        self.logger.error(f"Таблица {table_name}, батч {batch_num}, попытка {attempt + 1}: {str(e)}")
                        if attempt == max_retries - 1:
                            return 0, len(rows_batch)
                        time.sleep(retry_delay * (attempt + 1))

                return 0, len(rows_batch)

            try:
                result_set = self.session.execute(statement)
                page_num = 0

                while True:
                    page_num += 1
                    rows = list(result_set.current_rows)
                    if not rows:
                        break

                    # Разбиваем страницу на батчи
                    batches = []
                    for i in range(0, len(rows), batch_size):
                        batch_rows = rows[i:i + batch_size]
                        batch_num = f"{table_name}-{page_num}-{i // batch_size + 1}"
                        batches.append((batch_rows, batch_num))

                    # Выполняем батчи параллельно
                    with ThreadPoolExecutor(max_workers=concurrency) as executor:
                        future_to_batch = {
                            executor.submit(execute_delete_batch, batch_rows, batch_num): (batch_rows, batch_num)
                            for batch_rows, batch_num in batches
                        }

                        for future in as_completed(future_to_batch):
                            try:
                                successful, failed = future.result()
                                total_deleted += successful
                            except Exception as e:
                                self.logger.error(f"Ошибка при выполнении батча для {table_name}: {e}")

                    # Переходим к следующей странице
                    if result_set.has_more_pages:
                        result_set = self.session.execute(statement, paging_state=result_set.paging_state)
                    else:
                        break

            except Exception as e:
                self.logger.error(f"Ошибка при удалении из таблицы {table_name}: {e}")

            elapsed = time.time() - start_time
            self.logger.info(f"Таблица {table_name}: удалено {total_deleted} записей за {elapsed:.2f} сек")
            return table_name, total_deleted

        # Обрабатываем все таблицы параллельно
        total_start_time = time.time()
        self.logger.info(f"Начинаю батчевое удаление из {len(table_names)} таблиц")

        with ThreadPoolExecutor(max_workers=min(len(table_names), concurrency)) as executor:
            future_to_table = {
                executor.submit(process_table, table_name): table_name
                for table_name in table_names
            }

            for future in as_completed(future_to_table):
                try:
                    table_name, deleted_count = future.result()
                    results[table_name] = deleted_count
                except Exception as e:
                    table_name = future_to_table[future]
                    self.logger.error(f"Критическая ошибка при обработке таблицы {table_name}: {e}")
                    results[table_name] = 0

        total_elapsed = time.time() - total_start_time
        total_deleted = sum(results.values())
        self.logger.info(
            f"Батчевое удаление завершено: {total_deleted} записей из {len(table_names)} таблиц за {total_elapsed:.2f} сек")

        return results

    def clear_tables(self, table_names: List[str], **delete_kwargs) -> Dict[str, Any]:
        """
        Быстрая очистка нескольких таблиц одновременно.
        """
        start_time = time.time()

        self.logger.info(f"Быстрая очистка {len(table_names)} таблиц: {', '.join(table_names)}")

        # Оптимальные параметры для батчевого удаления
        default_params = {
            'batch_size': 150,
            'concurrency': 8,
            'page_size': 1500,
            'max_retries': 3
        }
        params = {**default_params, **delete_kwargs}

        results = self.delete_all_rows_batch_multiple(table_names, **params)

        execution_time = time.time() - start_time

        # Формируем детальный отчет
        report = {
            'execution_time': execution_time,
            'total_deleted': sum(results.values()),
            'tables_processed': len(results),
            'results_by_table': results
        }

        self.logger.info(f"Очистка {len(table_names)} таблиц завершена за {execution_time:.2f} сек")
        self.logger.info(f"Всего удалено записей: {report['total_deleted']}")

        for table_name, deleted_count in results.items():
            self.logger.info(f"  {table_name}: {deleted_count} записей")

        return report

    # Также можно добавить метод для очистки одной таблицы (обратная совместимость)
    def clear_table(self, table_name: str, **delete_kwargs) -> Dict[str, Any]:
        """
        Быстрая очистка одной таблицы.
        """
        results = self.clear_tables([table_name], **delete_kwargs)

        # Возвращаем результат в том же формате, что и раньше
        return {
            'execution_time': results['execution_time'],
            'deleted_count': results['results_by_table'].get(table_name, 0)
        }


if __name__ == '__main__':

    from common.bindings.vault_for_ci import Vault
    from common.configs.vault_config import VaultCorp

    vault = Vault()

    State.test_user = vault.get_secret_value(
        path=VaultCorp.sa0000datarivertest,
        key="user"
    )
    State.test_password = vault.get_secret_value(
        path=VaultCorp.sa0000datarivertest,
        key="pass"
    )

    with Cassandra() as db:
        # rows = db.select('msisdn_map', condition='msisdn = 79541178188')
        # for row in rows:
        #     print(row)
        #
        # db.delete(
        #     table_name="msisdn_map",
        #     condition=f"msisdn = 79541178188"
        # )
        #
        # rows = db.select('msisdn_map', condition='msisdn = 79541178188')
        # for row in rows:
        #     print(row)

        rows = db.select(table_name="cpo_device")
        for row in rows:
            print(row)


