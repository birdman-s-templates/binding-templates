from dataclasses import is_dataclass, fields
from thrift.protocol import TCompactProtocol
from thrift.transport import TTransport
from typing import Dict, Type

from thrift.Thrift import TType


def encode_thrift(data, model: Type):

    # Создаем экземпляр модели Thrift, переданной в качестве аргумента
    model_instance = model()

    if isinstance(data, dict):
        for key, value in data.items():
            setattr(model_instance, key, value)

    elif is_dataclass(data):
        for field in fields(data):
            value = getattr(data, field.name)
            setattr(model_instance, field.name, value)
    else:
        raise ValueError("Объект должен быть dict или dataclass")

    # Создаем буфер TMemoryBuffer для хранения сериализованных данных
    msg_bytes = TTransport.TMemoryBuffer()

    # Создаем протокол TCompactProtocol для сериализации в буфер
    protocol_out = TCompactProtocol.TCompactProtocol(msg_bytes)

    # Записываем данные модели в протокол для сериализации
    model_instance.write(protocol_out)

    return msg_bytes.getvalue()

def encode_thrift_nested_data(data, model):
    """
    Обновленная функция энкодинга, которая учитывает вложенные данные (словари/кортежи/сеты)
    """

    if not isinstance(data, dict):
        raise ValueError("Data must be a dict")

    instance = model()
    thrift_spec = getattr(model, 'thrift_spec', None)
    if thrift_spec is None:
        raise ValueError("Model has no thrift_spec")

    # Обходим все поля по индексу (field id)
    for fid in range(1, len(thrift_spec)):
        field_spec = thrift_spec[fid]
        if field_spec is None:
            continue

        _, ftype, fname, ftype_info, _ = field_spec

        if fname not in data:
            continue  # Поле не передано — оставляем как есть (None по умолчанию)

        raw_value = data[fname]

        # Рекурсивное преобразование значений
        def convert_value(ftype, ftype_info, value):
            if value is None:
                return None

            if ftype == TType.STRUCT:
                if isinstance(value, dict):
                    # Создаём экземпляр вложенной структуры
                    struct_class = ftype_info[0]  # [InnerObject, None]
                    nested = struct_class()
                    nested_thrift_spec = getattr(struct_class, 'thrift_spec', None)
                    if nested_thrift_spec is None:
                        raise ValueError(f"Nested struct {struct_class} has no thrift_spec")

                    for nested_fid in range(1, len(nested_thrift_spec)):
                        nested_spec = nested_thrift_spec[nested_fid]
                        if nested_spec is None:
                            continue
                        _, nftype, nfname, nftype_info, _ = nested_spec
                        if nfname in value:
                            setattr(nested, nfname, convert_value(nftype, nftype_info, value[nfname]))
                    return nested
                elif hasattr(value, 'write') and hasattr(value, 'thrift_spec'):
                    # Уже Thrift-объект — оставляем как есть
                    return value
                else:
                    raise TypeError(f"Expected dict or Thrift struct for field {fname}, got {type(value)}")

            elif ftype == TType.LIST:
                if isinstance(value, list):
                    item_type = ftype_info[0]
                    item_type_info = ftype_info[1] if len(ftype_info) > 1 else None
                    return [convert_value(item_type, item_type_info, item) for item in value]
                else:
                    raise TypeError(f"Expected list for field {fname}")

            elif ftype == TType.SET:
                if isinstance(value, (set, list)):
                    # Thrift в Python принимает set, но часто удобнее передавать list
                    item_type = ftype_info[0]
                    item_type_info = ftype_info[1] if len(ftype_info) > 1 else None
                    converted = {convert_value(item_type, item_type_info, item) for item in value}
                    return converted
                else:
                    raise TypeError(f"Expected set or list for field {fname}")

            elif ftype == TType.MAP:
                if isinstance(value, dict):
                    ktype, vtype = ftype_info[0], ftype_info[1]
                    ktype_info = ftype_info[2] if len(ftype_info) > 2 else None
                    vtype_info = ftype_info[3] if len(ftype_info) > 3 else None
                    return {
                        convert_value(ktype, ktype_info, k): convert_value(vtype, vtype_info, v)
                        for k, v in value.items()
                    }
                else:
                    raise TypeError(f"Expected dict for map field {fname}")

            else:
                # Простые типы: i32, string, bool и т.д.
                return value

        converted_value = convert_value(ftype, ftype_info, raw_value)
        setattr(instance, fname, converted_value)

    # Сериализация
    transport = TTransport.TMemoryBuffer()
    protocol = TCompactProtocol.TCompactProtocol(transport)
    instance.write(protocol)
    return transport.getvalue()

def encode_protobuf(data: dict, model: Type) -> bytes:
    # Создаём экземпляр переданного protobuf-класса
    message_instance = model()

    # Устанавливаем значения для каждого атрибута, если он присутствует в data
    for key, value in data.items():
        if hasattr(message_instance, key):
            setattr(message_instance, key, value)

    return message_instance.SerializeToString()
