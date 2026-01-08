import base64
import logging
import os
import subprocess
import tempfile


class Kerberos:

    def __init__(self, user: str = "sa0000datariverprep", keytab_base64=None, keytab_path=None):
        self.user = user
        self.keytab_base64 = keytab_base64
        if keytab_base64:
            keytab_bytes = base64.b64decode(keytab_base64)
            temp_keytab = tempfile.NamedTemporaryFile(delete=False)
            temp_keytab.write(keytab_bytes)
            temp_keytab.flush()
            temp_keytab.close()
            self.keytab_path = temp_keytab.name
        else:
            self.keytab_path = keytab_path

        self.krb5ccname = None
        self._file_path = None  # Хранит чистый путь (без префикса)
        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(logging.INFO)

    def kinit(self):
        # Генерируем УНИКАЛЬНЫЙ путь с PID для параллельных процессов
        self._file_path = f"/tmp/krb5cc_{os.getuid()}_{os.getpid()}"
        self.krb5ccname = f"FILE:{self._file_path}"  # Добавляем префикс FILE:
        os.environ["KRB5CCNAME"] = self.krb5ccname

        kinit_command = f'kinit -kt {self.keytab_path} {self.user} -c {self._file_path}'
        try:
            self.logger.info(f'Выполнение команды: {kinit_command}')
            subprocess.check_output(kinit_command, shell=True, stderr=subprocess.STDOUT, universal_newlines=True)
            self.logger.info(f'Kerberos ticket получен (кэш: {self._file_path})')
        except subprocess.CalledProcessError as error:
            raise RuntimeError(f'Ошибка kinit: {error.output}')

        # Проверяем содержимое кэша
        klist_output = subprocess.check_output(
            f"klist -c {self._file_path}",
            shell=True,
            stderr=subprocess.STDOUT,
            universal_newlines=True
        )
        self.logger.info(f"Содержимое кэша:\n{klist_output}")

    def kdestroy(self):
        try:
            # Уничтожаем ТОЛЬКО свой кэш (с уникальным PID)
            subprocess.check_output(
                f"kdestroy -c {self._file_path}",
                shell=True,
                stderr=subprocess.STDOUT,
                universal_newlines=True
            )
            self.logger.info(f'Кэш Kerberos уничтожен: {self._file_path}')
        except subprocess.CalledProcessError as error:
            self.logger.warning(f'kdestroy не смог удалить кэш: {error.output}')
        finally:
            # Удаляем временный keytab (если он был создан)
            if self.keytab_path and os.path.exists(self.keytab_path) and self.keytab_base64:
                os.remove(self.keytab_path)
                self.logger.info(f'Временный keytab удален: {self.keytab_path}')

    def __enter__(self):
        self.kinit()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.kdestroy()
