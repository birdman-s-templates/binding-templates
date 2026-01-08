import logging
from google.protobuf import timestamp_pb2

from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from typing import Dict, Type


def decode_thrift(message: bytes, model: Type) -> Dict:

    # Создаем экземпляр модели Thrift, переданной в качестве аргумента
    model_instance = model()

    # Создаем буфер TMemoryBuffer из переданных байтов
    msg_bytes = TTransport.TMemoryBuffer(message)

    # Создаем протокол TCompactProtocol из буфера
    protocol_in = TCompactProtocol.TCompactProtocol(msg_bytes)

    # Читаем данные из протокола в созданный экземпляр модели
    model_instance.read(protocol_in)

    return model_instance.__dict__


def decode_protobuf(message: bytes, model: Type) -> object:
    logger = logging.getLogger('decode_protobuf')
    logger.setLevel(logging.INFO)

    message_instance = model()
    message_instance.ParseFromString(message)

    # Создаем пустой словарь для результата
    result = {}

    # Итерируем по полям и проверяем их тип
    for field in message_instance.DESCRIPTOR.fields:
        field_value = getattr(message_instance, field.name)

        # Если поле типа Timestamp, преобразуем его в datetime
        if isinstance(field_value, timestamp_pb2.Timestamp):
            field_value = field_value.ToDatetime()

        # Добавляем результат в словарь
        result[field.name] = field_value

    return result
