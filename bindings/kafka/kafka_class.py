import csv
import datetime
import json
import logging
import io
import time
from typing import Optional
from confluent_kafka import Consumer, Producer, KafkaError, TopicPartition, KafkaException

from bindings.kafka.decoders import decode_protobuf, decode_thrift
from common.configs import kafka_config
from common.utils.state import State


class Kafka:
    def __init__(self, bootstrap_servers=None, security_protocol="SASL_PLAINTEXT", sasl_mechanism="GSSAPI",
                 sasl_plain_username=None, sasl_plain_password=None):

        base_config = {
            'bootstrap.servers': ', '.join(bootstrap_servers),
            'security.protocol': security_protocol,
            'sasl.mechanism': sasl_mechanism,
            'linger.ms': 0,  # отправлять сразу
            'sasl.kerberos.service.name': 'kafka',
            'sasl.kerberos.kinit.cmd': ' ',
            'auto.offset.reset': 'latest',
            'enable.auto.commit': True,
            'group.id': 'kafka',
        }

        if State.is_gitlab_runner:
            self.config = {
                **base_config,
                'bootstrap.servers': ', '.join(kafka_config.KAFKA_PREP1),
                'sasl.username': sasl_plain_username if sasl_plain_username is not None else State.admin_user,
                'sasl.password': sasl_plain_password if sasl_plain_password is not None else State.admin_password
            }
        else:

            self.config = {
                **base_config,
            }
        self.producer: Optional[Producer] = None
        self.consumer: Optional[Consumer] = None
        self.logger = logging.getLogger('Kafka')
        self.logger.setLevel(logging.DEBUG)

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.producer:
            self.producer.flush()
        if self.consumer:
            self.consumer.close()

        self.producer = None
        self.consumer = None

    def create_producer(self, linger_ms=0, batch_size=1000000, message_max_bytes=100000, batch_num_messages=10000,
                        is_batch=False):

        """
        linger_ms (int, optional): Время ожидания перед отправкой данных (по умолчанию 0).
        batch_size (int, optional): Размер пакета для батчей (по умолчанию 1000000).
        messages_max_bytes (int, optional): Максимальный размер сообщения в байтах (по умолчанию 100000).
        batch_num_messages (int, optional): Число сообщений в батче (по умолчанию 10000).
        is_batch (bool, optional): Флаг для использования батчей (по умолчанию False).
        """
        try:
            if is_batch:
                self.config = {**self.config} | {
                    'linger.ms': linger_ms,
                    'batch.size': batch_size,
                    'message.max.bytes': message_max_bytes,
                    'batch.num.messages': batch_num_messages,
                    'compression.type': 'lz4',
                }
            self.producer = Producer(**self.config)
            self.logger.info(f"Producer успешно создан ({datetime.datetime.now()})")
        except KafkaException as e:
            self.logger.error(f"Ошибка инициализации Producer: {e}")

    def create_consumer(self, topic, sasl_kerberos_service_name="kafka", auto_offset_reset='latest',
                        enable_auto_commit=True,
                        connections_max_idle_ms=120000,
                        request_timeout_ms=110000,
                        retries=5,
                        **kwargs):

        self.config = {**self.config} | {
            'sasl.kerberos.service.name': sasl_kerberos_service_name,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': enable_auto_commit,
            'retries': retries,
            'request.timeout.ms': request_timeout_ms,
            'connections.max.idle.ms': connections_max_idle_ms,
            **kwargs,
        }
        try:
            self.consumer = Consumer(**self.config)
            partitions = [TopicPartition(topic, partition) for partition in
                          self.consumer.list_topics(topic).topics[topic].partitions]

            self.consumer.assign(partitions)
            self.logger.info(f"Consumer создан для топика ({datetime.datetime.now()}): {topic}")
        except KafkaException as e:
            self.logger.error(f"Ошибка инициализации Consumer: {e}")

    def send_message(self, topic, message, key=None, timeout=None):
        if self.producer is None:
            raise RuntimeError("Kafka is not running")
        try:
            if isinstance(key, str):
                key = key.encode('utf-8')
            elif isinstance(key, int):
                key = bytes(str(key), 'utf-8')
            self.logger.info(f"Попытка отправить сообщение в топик ({datetime.datetime.now()}): {topic}")
            self.producer.produce(topic=topic, key=key, value=message)
            if timeout is not None:
                remaining = self.producer.flush(timeout=timeout)
            else:
                remaining = self.producer.flush()

            if remaining > 0:
                raise TimeoutError(f"Не удалось отправить {remaining} сообщений за {timeout}с")

            self.logger.info(f"Сообщение отправлено в топик ({datetime.datetime.now()}): {topic}. Ключ: {key}.")
        except KafkaError as e:
            self.logger.error(f"Ошибка при отправке сообщения: {e}")
            raise e

    def send_message_batch(self, topic, messages, timeout=None):
        """Отправка пакета сообщений в Kafka-топик.
        Parameters:
        topic (str): Топик, в который нужно отправить сообщения.
        messages (list): Список словарей, каждый из которых содержит 'key' и 'value'.
        timeout (int, optional): Время ожидания завершения отправки сообщений в секундах. По умолчанию None.
        """
        if not self.producer:
            raise RuntimeError("Kafka is not running")
        failed = []
        try:
            for i, message in enumerate(messages):
                try:
                    key = message.get('key')
                    if isinstance(key, str):
                        key = key.encode('utf-8')
                    elif isinstance(key, int):
                        key = bytes(str(key), 'utf-8')
                    self.producer.produce(topic=topic, key=key, value=message.get('value'))
                except BufferError as e:
                    self.producer.poll(0)
                    try:
                        self.producer.produce(topic=topic, key=key, value=message.get('value'))
                    except Exception as e:
                        failed.append((i, message, str(e)))
                except Exception as e:
                    failed.append((i, message, str(e)))
            if timeout is not None:
                remaining = self.producer.flush(timeout=timeout)
            else:
                remaining = self.producer.flush()

            if remaining > 0:
                raise TimeoutError(f"Не удалось отправить {remaining} сообщений за {timeout}с")

            if failed:
                raise RuntimeError(
                    f"Не удалось отправить {len(failed)} сообщений: {failed}"
                )
        except KafkaError as e:
            raise e

    def find_message_by_key(self, message_key=None, is_message_key_int: bool = False, search_depth=1000,
                            return_all=False,
                            max_wait_time=None, decoding_model=None, protobuf=False, binary=False, send_function=None,
                            timeout_sec=10, timeout_ms=None, key_for_search=None, **kwargs):
        """
        Функция, которая подключаются к топику для чтения, а сразу после отправляет сообщение в целевой топик
        (может быть другим), это очень актуально для тестов, где триггер для получения сообщения - отправка другого.

        Сообщения ищутся по ключу.

        - message_key: ключ в отправленном сообщении - использовать в producer
        - key_for_search: ключ для поиска целевого сообщения - использовать в consumer
        - search_depth: максимальное количество проверяемых сообщений
        - return_all: если True, возвращает все сообщения с указанным ключом (в виде списка)
        - max_wait_time: максимальное время (в секундах) на выполнение поиска
        - decoding_model: модель для декодирования (используется для Thrift или Protobuf)
        - protobuf: если True, используется Protobuf декодирование
        - thrift: если True, используется Thrift декодирование
        - send_function: метод продюсера, используются отдельный экземпляр Кафки т.к. кластеры могут отличаться для
        продюсера и консьюмера
        - timeout_sec: время ожидания сообщений в секундах (приоритетнее timeout_ms)
        - timeout_ms: время ожидания сообщений в миллисекундах
        - kwargs: должны быть send_topic и message для отправки

        Если что-то передано в decoding_model, но False у protobuf и binary, то thrift. Подробнее это описано в методе
        __decoding.
        """

        if not self.consumer:
            raise Exception("Consumer не был инициализирован. Сначала нужно вызвать create_consumer()")

        try:
            counter = 0
            start_time = time.time()
            found_messages = [] if return_all else None

            send_flag = False

            key = key_for_search if key_for_search is not None else message_key
            key_for_search = key

            # Преобразование ключа в bytes, если это строка или число
            if isinstance(key_for_search, str):
                key_for_search = key_for_search.encode('utf-8')
            elif isinstance(key_for_search, int):
                key_for_search = bytes(str(key_for_search), 'utf-8')

            if is_message_key_int and isinstance(message_key, int):
                message_key = message_key.to_bytes(8, byteorder='big', signed=False)

            elif not is_message_key_int:
                if isinstance(message_key, str):
                    message_key = message_key.encode('utf-8')
                elif isinstance(message_key, int):
                    message_key = bytes(str(message_key), 'utf-8')

            if timeout_sec is not None:
                timeout = timeout_sec
            else:
                timeout = timeout_ms / 1000.0

            while counter < search_depth:

                # Проверка на время выполнения
                if max_wait_time is not None and (time.time() - start_time) >= max_wait_time:
                    break

                messages_left_to_check = search_depth - counter

                num_messages = min(messages_left_to_check, 50)
                num_messages = max(num_messages, 1)

                messages = self.__consume_messages_batch(num_messages=num_messages, timeout=timeout)

                # Если после начала чтения топика нужно отправить в него сообщения
                if send_function is not None and not send_flag:
                    send_function(key=message_key, **kwargs)
                    send_flag = True
                # Если нет новых сообщений, продолжаем ожидание
                if not messages:
                    continue

                # Обрабатываем каждое сообщение в записи
                for message in messages:
                    # Проверка ключа сообщения
                    if message.key() == key_for_search:
                        decoded_message = self.__decoding(message_value=message.value(),
                                                          decoding_model=decoding_model,
                                                          protobuf=protobuf, binary=binary)

                        if return_all:
                            found_messages.append(decoded_message)
                        else:
                            return decoded_message

                    counter += 1
                    if counter >= search_depth:
                        break

                if counter >= search_depth:
                    break

            return found_messages if return_all else None

        except Exception as e:
            self.logger.error(f"Ошибка при чтении сообщений: {e}")
            raise

    def send_and_search_by_json(self, *search_criteria, send_function=None, max_messages_search=1, search_depth=100000,
                                offset=0, max_wait_time=60, decoding_model=None, protobuf=False, binary=False,
                                timeout_sec=None, timeout_ms=1000, **kwargs):
        """
        Функция, которая подключаются к топику для чтения, а сразу после отправляет сообщение в целевой топик
        (может быть другим), это очень актуально для тестов, где триггер для получения сообщения - отправка другого.

        Сообщения ищутся по параметрам из сообщения.
        - search_criteria - кортежи, в которых лежат пары название поля - значение для поиска
        - send_function: метод продюсера, используются отдельный экземпляр Кафки т.к. кластеры могут отличаться для
        продюсера и консьюмера
        - search_depth: максимальное количество проверяемых сообщений
        - decoding_model: модель для декодирования (используется для Thrift или Protobuf)
        - protobuf: если True, используется Protobuf декодирование
        - timeout_sec: время ожидания сообщений в секундах (приоритетнее timeout_ms)
        - timeout_ms: время ожидания сообщений в миллисекундах
        - max_wait_time: максимальное время (в секундах) на выполнение поиска
        - max_messages_search: сколько сообщений максимум нужно искать
        - kwargs: должны быть send_topic и message для отправки

        Если что-то передано в decoding_model, но False у protobuf и binary, то thrift. Подробнее это описано в методе
        __decoding.
        """
        if not self.consumer:
            raise Exception("Consumer не был инициализирован. Сначала нужно вызвать create_consumer()")

        try:
            self.consumer.poll(0)

            if offset > 0:
                partitions = self.consumer.assignment()
                for partition in partitions:
                    low_watermark, high_watermark = self.consumer.get_watermark_offsets(partition)

                    new_offset = high_watermark - offset if (high_watermark - offset) > low_watermark else low_watermark

                    self.consumer.seek(
                        TopicPartition(topic=partition.topic, partition=partition.partition, offset=new_offset))

            messages = []
            checked_messages = 0
            start_time = time.time()

            flag = False

            self.logger.info(f"send_and_search_by_json: Поиск сообщения по {search_criteria}")

            if timeout_sec is not None:
                timeout = timeout_sec
            else:
                timeout = timeout_ms / 1000.0
            while len(messages) < max_messages_search and checked_messages < search_depth:
                messages_needed = max_messages_search - len(messages)
                messages_left_to_check = search_depth - checked_messages

                num_messages = min(messages_needed, messages_left_to_check, 100)
                num_messages = max(num_messages, 1)

                message_pack = self.__consume_messages_batch(num_messages=num_messages, timeout=timeout)

                if not flag:
                    send_function(**kwargs)
                    flag = True

                if not message_pack:
                    if (time.time() - start_time) > max_wait_time:
                        self.logger.info(f"Превышено максимальное время ожидания сообщений ({datetime.datetime.now()})")
                        break
                    continue

                for message in message_pack:
                    checked_messages += 1
                    try:
                        msg_value = self.__decoding(message_value=message.value(),
                                                    decoding_model=decoding_model,
                                                    protobuf=protobuf, binary=binary)

                        if all(msg_value.get(key) == value for key, value in search_criteria):
                            self.logger.info(f"send_and_search_by_json: сообщение найдено")
                            messages.append(msg_value)
                            if len(messages) >= max_messages_search:
                                break

                        else:
                            self.logger.info(f"send_and_search_by_json: сообщение НЕ найдено")

                    except json.JSONDecodeError as e:
                        self.logger.error(f"Не удалось декодировать json ({datetime.datetime.now()}): {e}")
                        continue
                    if len(messages) >= max_messages_search or checked_messages >= search_depth:
                        break

            if messages:
                self.logger.info(f"Найдено сообщений: {len(messages)}")
            else:
                self.logger.info(f"Сообщений не найдено")
            return messages
        except Exception as e:
            self.logger.error(f"Ошибка при чтении сообщений ({datetime.datetime.now()}): {e}")
            raise

    def read_messages(self, depth=100, decoding_model=None, protobuf=False, binary=False, timeout=120):
        if not self.consumer:
            raise Exception("Consumer не был инициализирован. Сначала нужно вызвать create_consumer()")
        result = []
        try:
            messages = self.__consume_messages_batch(num_messages=depth, timeout=timeout)
            if messages:
                for i, message in enumerate(messages):
                    msg_value = self.__decoding(message_value=message.value(),
                                                decoding_model=decoding_model,
                                                protobuf=protobuf, binary=binary)
                    result.append(msg_value)
            else:
                self.logger.error(f"Сообщений не найдено")
            return result

        except Exception as e:
            self.logger.error(f"Ошибка при чтении сообщений ({datetime.datetime.now()}): {e}")
            raise

    def __decoding(self, message_value, decoding_model, protobuf: bool, binary: bool):
        """
        Декодирует сообщение в зависимости от заданных параметров.

        Есть важный нюанс:
        - если что-то передано в decoding_model, то месседж точно в protobuf, thrift или binary, но во входных
        параметрах имеется только protobuf и binary, это сделано для обратной совместимости, т.к. много тестов, что
        работают с thrift, но при вызове функции не передается параметр thrift (не хотелось ломать), поэтому, если
        decoding_model есть, но нет protobuf и binary, значит, это обработается как thrift
        """
        try:
            if decoding_model is not None:
                if protobuf:
                    self.logger.info(f"send_and_search_by_json: protobuf")
                    return decode_protobuf(message=message_value, model=decoding_model)
                elif binary:
                    return decoding_model(message_value)
                else:
                    self.logger.info(f"send_and_search_by_json: thrift")
                    return decode_thrift(message=message_value, model=decoding_model)
            else:
                self.logger.info(f"send_and_search_by_json: json")
                return json.loads(message_value.decode('utf-8'))
        except UnicodeDecodeError:
            # CSV или другое (но ожидаем CSV)
            self.logger.info(f"send_and_search_by_json: CSV")
            return [row for row in csv.reader(io.StringIO(message_value))]
        except Exception as e:
            self.logger.error(f"Ошибка при декодировании сообщения: {e}")
            return message_value

    def __consume_messages_batch(self, num_messages, timeout):
        """Функция для отбора сообщений без ошибок"""
        messages = self.consumer.consume(num_messages=num_messages, timeout=timeout)
        if not messages:
            return []
        valid_messages = []
        for message in messages:
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:  # _PARTITION_EOF | Broker: No more messages
                    continue
                else:
                    self.logger.error(f"Ошибка при получении сообщения: {message.error()}")
                    continue

            valid_messages.append(message)
        return valid_messages
