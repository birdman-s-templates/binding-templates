from pytest_check import check


def __deep_equal(expected, actual, ignore_order, ignore_duplicates, ignore_params=None):
    """
    Рекурсивное сравнение двух объектов: dict, list и простых типов,
    с учётом списка ключей ignore_params, которые нужно пропускать.
    """
    if type(expected) != type(actual):
        return False

    if isinstance(expected, dict):
        # Формируем множества ключей без игнорируемых
        exp_keys = {k for k in expected if not (ignore_params and k in ignore_params)}
        act_keys = {k for k in actual   if not (ignore_params and k in ignore_params)}
        if exp_keys != act_keys:
            return False
        for k in exp_keys:
            if not __deep_equal(
                expected[k], actual[k],
                ignore_order, ignore_duplicates, ignore_params
            ):
                return False
        return True

    elif isinstance(expected, list):
        if ignore_order:
            if ignore_duplicates:
                # Сравнение уникальных элементов
                unique_expected = []
                for e in expected:
                    if not any(__deep_equal(e, ue, ignore_order, ignore_duplicates, ignore_params)
                               for ue in unique_expected):
                        unique_expected.append(e)
                unique_actual = []
                for a in actual:
                    if not any(__deep_equal(a, ua, ignore_order, ignore_duplicates, ignore_params)
                               for ua in unique_actual):
                        unique_actual.append(a)
                if len(unique_expected) != len(unique_actual):
                    return False
                for e in unique_expected:
                    if not any(__deep_equal(e, a, ignore_order, ignore_duplicates, ignore_params)
                               for a in unique_actual):
                        return False
                return True
            else:
                # Сравнение с учётом кратности
                if len(expected) != len(actual):
                    return False
                actual_copy = actual.copy()
                for e in expected:
                    found = False
                    for idx, a in enumerate(actual_copy):
                        if __deep_equal(e, a, ignore_order, ignore_duplicates, ignore_params):
                            del actual_copy[idx]
                            found = True
                            break
                    if not found:
                        return False
                return True
        else:
            # Сравнение по порядку
            if len(expected) != len(actual):
                return False
            for i in range(len(expected)):
                if not __deep_equal(expected[i], actual[i], ignore_order, ignore_duplicates, ignore_params):
                    return False
            return True
    else:
        return expected == actual


def __strings_equals(path: str, expected: str, actual: str):
    if expected is None or actual is None:
        check.fail(f"{path}: одна из строк равна None (ожидалось={expected}, получено={actual})")
    else:
        check.equal(expected, actual,
                    f"{path}: строковое значение не совпадает. Ожидалось: {expected}, получено: {actual}")


def __bool_equals(path: str, expected: bool, actual: bool):
    if expected is None or actual is None:
        check.fail(f"{path}: одно из булевых значений равно None (ожидалось={expected}, получено={actual})")
    else:
        check.equal(expected, actual,
                    f"{path}: булевыe значения не совпадают. Ожидалось: {expected}, получено: {actual}")


def __int_equals(path: str, expected: int, actual: int):
    if expected is None or actual is None:
        check.fail(f"{path}: одно из целочисленных значений равно None (ожидалось={expected}, получено={actual})")
    else:
        check.equal(expected, actual,
                    f"{path}: целочисленные значения не совпадают. Ожидалось: {expected}, получено: {actual}")


def __dicts_equals(path: str, expected: dict, actual: dict,
                   ignore_order=True, ignore_duplicates=False, ignore_params=None):
    if expected is None or actual is None:
        check.fail(f"{path}: один из словарей равен None (ожидалось={expected}, получено={actual})")

    # Проверяем ожидаемые ключи
    for key, exp_val in expected.items():
        if ignore_params and key in ignore_params:
            continue
        new_path = f"{path}.{key}" if path else key
        if key not in actual:
            check.fail(f"{new_path}: ключ отсутствует в фактическом словаре")
        compare_values(
            exp_val, actual[key],
            path=new_path,
            ignore_order=ignore_order,
            ignore_duplicates=ignore_duplicates,
            ignore_params=ignore_params
        )

    # Лишние ключи
    for key in actual:
        if ignore_params and key in ignore_params:
            continue
        if key not in expected:
            new_path = f"{path}.{key}" if path else key
            check.fail(f"{new_path}: неожиданно присутствует в фактическом словаре")


def __lists_equals(path: str, expected: list, actual: list,
                   ignore_order=True, ignore_duplicates=False, ignore_params=None):
    if expected is None or actual is None:
        check.fail(f"{path}: один из списков равен None (ожидалось={expected}, получено={actual})")

    # По порядку
    if not ignore_order:
        if len(expected) != len(actual):
            check.fail(f"{path}: длины списков различаются (ожидалось {len(expected)}, получено {len(actual)})")
        for i, (e, a) in enumerate(zip(expected, actual)):
            compare_values(
                e, a,
                path=f"{path}[{i}]",
                ignore_order=ignore_order,
                ignore_duplicates=ignore_duplicates,
                ignore_params=ignore_params
            )
        return

    # Без учёта порядка, только уникальные
    if ignore_duplicates:
        unique_expected = []
        for e in expected:
            if not any(__deep_equal(e, ue, ignore_params) for ue in unique_expected):
                unique_expected.append(e)
        unique_actual = []
        for a in actual:
            if not any(__deep_equal(a, ua, ignore_params) for ua in unique_actual):
                unique_actual.append(a)
        if len(unique_expected) != len(unique_actual):
            check.fail(f"{path}: количество уникальных элементов различается "
                       f"(ожидалось {len(unique_expected)}, получено {len(unique_actual)})")
        for e in unique_expected:
            if not any(__deep_equal(e, a, ignore_params) for a in unique_actual):
                check.fail(f"{path}: уникальный элемент {e} отсутствует в списке")
        return
    else:
        if len(expected) != len(actual):
            check.fail(f"{path}: длины списков различаются (ожидалось {len(expected)}, получено {len(actual)})")
        actual_copy = actual.copy()
        for e in expected:
            found = False
            for idx, a in enumerate(actual_copy):
                if __deep_equal(e, a, ignore_order, ignore_duplicates, ignore_params):
                    del actual_copy[idx]
                    found = True
                    break
            if not found:
                check.fail(f"{path}: элемент {e} не найден в списке (без учёта порядка)")


def compare_values(expected, actual, path: str = "",
                   ignore_order=True, ignore_duplicates=False, ignore_params=None):
    """
    Универсальная функция для рекурсивного сравнения значений разных типов.
    """
    if isinstance(expected, dict) and isinstance(actual, dict):
        __dicts_equals(path, expected, actual,
                       ignore_order=ignore_order,
                       ignore_duplicates=ignore_duplicates,
                       ignore_params=ignore_params)
    elif isinstance(expected, list) and isinstance(actual, list):
        __lists_equals(path, expected, actual,
                       ignore_order=ignore_order,
                       ignore_duplicates=ignore_duplicates,
                       ignore_params=ignore_params)
    elif isinstance(expected, str) and isinstance(actual, str):
        __strings_equals(path, expected, actual)
    elif isinstance(expected, bool) and isinstance(actual, bool):
        __bool_equals(path, expected, actual)
    elif isinstance(expected, int) and isinstance(actual, int):
        __int_equals(path, expected, actual)
    else:
        check.equal(expected, actual,
                    f"{path}: несоответствие типов или значений (ожидалось {expected}, получено {actual})")
